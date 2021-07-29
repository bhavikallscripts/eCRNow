package com.drajer.bsa.ehr.service.impl;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import com.drajer.bsa.ehr.service.EhrAuthorizationService;
import com.drajer.bsa.ehr.service.EhrQueryService;
import com.drajer.bsa.model.KarProcessingData;
import com.drajer.bsa.utils.BsaServiceUtils;
import com.drajer.sof.utils.FhirContextInitializer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.codec.binary.Base64;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

/**
 *
 *
 * <h1>EhrQueryService</h1>
 *
 * This class defines the implementation methods to access data from the Ehr for a set of resources.
 *
 * @author nbashyam
 */
@Service
@Transactional
public class EhrFhirR4QueryServiceImpl implements EhrQueryService {

  private final Logger logger = LoggerFactory.getLogger(EhrFhirR4QueryServiceImpl.class);

  /** The EHR Authorization Service class enables the BSA to get an access token. */
  @Autowired EhrAuthorizationService authorizationService;

  private static final String R4 = "R4";
  private static final String PATIENT_RESOURCE = "Patient";
  private static final String PATIENT_CONTEXT = "patientContext";
  private static final String PATIENT_ID_SEARCH_PARAM = "?patient=";

  /** The FHIR Context Initializer necessary to retrieve FHIR resources */
  @Autowired FhirContextInitializer fhirContextInitializer;

  @Autowired BsaServiceUtils bsaServerUtils;

  @Value("${basicAuth}")
  private Boolean basicAuth;

  @Value("${authentication.username}")
  private String username;

  @Value("${authentication.password}")
  private String authPassword;

  @Autowired RestTemplate restTemplate;

  /**
   * The method is used to retrieve data from the Ehr.
   *
   * @param kd The processing context which contains information such as patient, encounter,
   *     previous data etc.
   * @return The Map of Resources to its type.
   */
  @Override
  public HashMap<ResourceType, Set<Resource>> getFilteredData(
      KarProcessingData kd, HashMap<String, ResourceType> resTypes) {

    logger.info(" Getting FHIR Context for R4");
    FhirContext context = fhirContextInitializer.getFhirContext(R4);

    // Get Patient by Id always
    Resource res =
        getResourceById(
            kd.getNotificationContext().getFhirServerBaseUrl(),
            context,
            PATIENT_RESOURCE,
            kd.getNotificationContext().getPatientId());
    if (res != null) {

      logger.info(
          " Found Patient resource for Id : {}", kd.getNotificationContext().getPatientId());

      Set<Resource> resources = new HashSet<Resource>();
      resources.add(res);
      HashMap<ResourceType, Set<Resource>> resMap = new HashMap<>();
      resMap.put(res.getResourceType(), resources);
      kd.addResourcesByType(resMap);
    }

    // Fetch Resources by Patient Id.
    for (Map.Entry<String, ResourceType> entry : resTypes.entrySet()) {

      logger.info(" Fetching Resource of type {}", entry.getValue());

      if (entry.getValue() != ResourceType.Patient || entry.getValue() != ResourceType.Encounter) {
        String url =
            kd.getNotificationContext().getFhirServerBaseUrl()
                + "/"
                + entry.getValue().toString()
                + PATIENT_ID_SEARCH_PARAM
                + kd.getNotificationContext().getPatientId();

        logger.info(" Resource Query Url : {}", url);

        getResourcesByPatientId(
            context, entry.getValue().toString(), url, kd, entry.getValue(), entry.getKey());
      }
    }

    // Get other resources for Patient
    return kd.getFhirInputData();
  }

  public Resource getResourceById(
      String baseUrl, FhirContext context, String resourceName, String resourceId) {

    Resource resource = null;
    HttpHeaders headers = new HttpHeaders();
    try {
      String password = authPassword + bsaServerUtils.convertDateToString();
      String encodedPassword = Base64.encodeBase64String(password.getBytes());
      headers.setBasicAuth(username, encodedPassword);
      HttpEntity entity = new HttpEntity(headers);

      String url = baseUrl + "/" + resourceName + "/" + resourceId;

      logger.info("Getting data for Resource : {} with Id : {}", resourceName, resourceId);
      ResponseEntity<String> response =
          restTemplate.exchange(url, HttpMethod.GET, entity, String.class);

      if (response.getBody() != null) {
        logger.info(
            "Received Status:::::{}, Response Body:::::{}",
            response.getStatusCode(),
            response.getBody());
        resource = (Resource) context.newJsonParser().parseResource(response.getBody());
      }
    } catch (BaseServerResponseException responseException) {
      if (responseException.getOperationOutcome() != null) {
        logger.debug(
            context
                .newJsonParser()
                .encodeResourceToString(responseException.getOperationOutcome()));
      }
      logger.error(
          "Error in getting {} resource by Id: {}", resourceName, resourceId, responseException);
    } catch (Exception e) {
      logger.error("Error in getting {} resource by Id: {}", resourceName, resourceId, e);
    }
    return resource;
  }

  public void getResourcesByPatientId(
      FhirContext context,
      String resourceName,
      String searchUrl,
      KarProcessingData kd,
      ResourceType resType,
      String id) {

    logger.info("Invoking search url : {}", searchUrl);
    Set<Resource> resources = null;
    HashMap<ResourceType, Set<Resource>> resMap = null;
    HashMap<String, Set<Resource>> resMapById = null;
    HttpHeaders headers = new HttpHeaders();
    try {
      logger.info(
          "Getting {} data using Patient Id: {}",
          resourceName,
          kd.getNotificationContext().getPatientId());

      String password = authPassword + bsaServerUtils.convertDateToString();
      String encodedPassword = Base64.encodeBase64String(password.getBytes());
      headers.setBasicAuth(username, encodedPassword);
      HttpEntity entity = new HttpEntity(headers);

      ResponseEntity<String> response =
          restTemplate.exchange(searchUrl, HttpMethod.GET, entity, String.class);
      if (response.getBody() != null) {
        logger.info(
            "Received Status:::::{}, Response Body:::::{}",
            response.getStatusCode(),
            response.getBody());
        Bundle bundle = (Bundle) context.newJsonParser().parseResource(response.getBody());

        getAllR4RecordsUsingPagination(bundle, context);

        if (bundle != null) {
          logger.info(
              "Total No of Entries {} retrieved : {}", resourceName, bundle.getEntry().size());

          List<BundleEntryComponent> bc = bundle.getEntry();

          if (bc != null) {

            resources = new HashSet<Resource>();
            resMap = new HashMap<>();
            resMapById = new HashMap<>();
            for (BundleEntryComponent comp : bc) {

              logger.info(" Adding Resource Id : {}", comp.getResource().getId());
              resources.add(comp.getResource());
            }

            resMap.put(resType, resources);
            resMapById.put(id, resources);
            kd.addResourcesByType(resMap);
            kd.addResourcesById(resMapById);

            logger.info(" Adding {} resources of type : {}", resources.size(), resType);
          } else {
            logger.error(" No entries found for type : {}", resType);
          }
        } else {
          logger.error(" Unable to retrieve resources for type : {}", resType);
        }
      }

    } catch (BaseServerResponseException responseException) {
      if (responseException.getOperationOutcome() != null) {
        logger.debug(
            context
                .newJsonParser()
                .encodeResourceToString(responseException.getOperationOutcome()));
      }
      logger.info(
          "Error in getting {} resource by Patient Id: {}",
          resourceName,
          kd.getNotificationContext().getPatientId(),
          responseException);
    } catch (Exception e) {
      logger.info(
          "Error in getting {} resource by Patient Id: {}",
          resourceName,
          kd.getNotificationContext().getPatientId(),
          e);
    }
  }

  private void getAllR4RecordsUsingPagination(Bundle bundle, FhirContext context) {

    HttpHeaders headers = new HttpHeaders();
    if (bundle.hasEntry()) {
      List<BundleEntryComponent> entriesList = bundle.getEntry();
      if (bundle.hasLink() && bundle.getLink(IBaseBundle.LINK_NEXT) != null) {
        logger.info(
            "Found Next Page in Bundle :{}", bundle.getLink(IBaseBundle.LINK_NEXT).getUrl());

        String password = authPassword + bsaServerUtils.convertDateToString();
        String encodedPassword = Base64.encodeBase64String(password.getBytes());
        headers.setBasicAuth(username, encodedPassword);
        HttpEntity entity = new HttpEntity(headers);

        ResponseEntity<String> response =
            restTemplate.exchange(
                bundle.getLink(IBaseBundle.LINK_NEXT).getUrl(),
                HttpMethod.GET,
                entity,
                String.class);
        if (response.getBody() != null) {
          logger.info(
              "Received Status:::::{}, Response Body:::::{}",
              response.getStatusCode(),
              response.getBody());
          Bundle nextPageBundleResults =
              (Bundle) context.newJsonParser().parseResource(response.getBody());
          entriesList.addAll(nextPageBundleResults.getEntry());
          nextPageBundleResults.setEntry(entriesList);
          getAllR4RecordsUsingPagination(nextPageBundleResults, context);
        }
      }
    }
  }
}
