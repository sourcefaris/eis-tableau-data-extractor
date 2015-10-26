package com.tableausoftware.documentation.api.rest.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.eis.dataextractor.LocalDBController;
import com.google.common.io.Files;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.MultiPartMediaTypes;
import com.sun.jersey.multipart.file.FileDataBodyPart;
import com.tableausoftware.documentation.api.rest.bindings.CapabilityType;
import com.tableausoftware.documentation.api.rest.bindings.DataSourceType;
import com.tableausoftware.documentation.api.rest.bindings.FileUploadType;
import com.tableausoftware.documentation.api.rest.bindings.GranteeCapabilitiesType;
import com.tableausoftware.documentation.api.rest.bindings.GroupType;
import com.tableausoftware.documentation.api.rest.bindings.ObjectFactory;
import com.tableausoftware.documentation.api.rest.bindings.ProjectListType;
import com.tableausoftware.documentation.api.rest.bindings.ProjectType;
import com.tableausoftware.documentation.api.rest.bindings.SiteType;
import com.tableausoftware.documentation.api.rest.bindings.TableauCredentialsType;
import com.tableausoftware.documentation.api.rest.bindings.TsRequest;
import com.tableausoftware.documentation.api.rest.bindings.TsResponse;
import com.tableausoftware.documentation.api.rest.bindings.UserType;

/**
 * This class encapsulates the logic used to make requests to the Tableau Server
 * REST API. This class is implemented as a singleton.
 */
public class RestApiUtils extends LocalDBController{
	
    private enum Operation {
        APPEND_FILE_UPLOAD(getApiUriBuilder().path("sites/{siteId}/fileUploads/{uploadSessionId}")),
        INITIATE_FILE_UPLOAD(getApiUriBuilder().path("sites/{siteId}/fileUploads")),
        PUBLISH_DATASOURCE(getApiUriBuilder().path("sites/{siteId}/datasources")),
        QUERY_PROJECTS(getApiUriBuilder().path("sites/{siteId}/projects")),
        SIGN_IN(getApiUriBuilder().path("auth/signin")),
        SIGN_OUT(getApiUriBuilder().path("auth/signout"));
        private final UriBuilder m_builder;
        Operation(UriBuilder builder) {
            m_builder = builder;
        }
        UriBuilder getUriBuilder() {
            return m_builder;
        }
        String getUrl(Object... values) {
            return m_builder.build(values).toString();
        }
    }

    private static RestApiUtils INSTANCE = null;
    private static Marshaller s_jaxbMarshaller;
    private static Unmarshaller s_jaxbUnmarshaller;

    public static RestApiUtils getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RestApiUtils();
            initialize();
        }

        return INSTANCE;
    }

    private static UriBuilder getApiUriBuilder() {
        return UriBuilder.fromPath(getPropertyValue("tableau.server.host") + "/api/2.0");
    }

    private static void initialize() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(TsRequest.class, TsResponse.class);
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = schemaFactory.newSchema(new File(getPropertyValue("tableau.schema.location")));
            s_jaxbMarshaller = jaxbContext.createMarshaller();
            s_jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            s_jaxbUnmarshaller.setSchema(schema);
            s_jaxbMarshaller.setSchema(schema);
        } catch (JAXBException | SAXException ex) {
            ex.printStackTrace();
        }
    }

    private final String TABLEAU_AUTH_HEADER = "X-Tableau-Auth";
    private final String TABLEAU_PAYLOAD_NAME = "request_payload";
    private Logger m_logger = Logger.getLogger(RestApiUtils.class);
    private ObjectFactory m_objectFactory = new ObjectFactory();

    private RestApiUtils() {}

    public GranteeCapabilitiesType createGroupGranteeCapability(GroupType group, Map<String, String> capabilitiesMap) {
        GranteeCapabilitiesType granteeCapabilities = m_objectFactory.createGranteeCapabilitiesType();

        // Sets the grantee to the specified group
        granteeCapabilities.setGroup(group);
        GranteeCapabilitiesType.Capabilities capabilities = m_objectFactory.createGranteeCapabilitiesTypeCapabilities();
        for (String key : capabilitiesMap.keySet()) {
            CapabilityType capabilityType = m_objectFactory.createCapabilityType();
            capabilityType.setName(key);
            capabilityType.setMode(capabilitiesMap.get(key));
            capabilities.getCapability().add(capabilityType);
        }
        granteeCapabilities.setCapabilities(capabilities);
        return granteeCapabilities;
    }
    
    public GranteeCapabilitiesType createUserGranteeCapability(UserType user, Map<String, String> capabilitiesMap) {
        GranteeCapabilitiesType granteeCapabilities = m_objectFactory.createGranteeCapabilitiesType();
        granteeCapabilities.setUser(user);
        GranteeCapabilitiesType.Capabilities capabilities = m_objectFactory.createGranteeCapabilitiesTypeCapabilities();
        for (String key : capabilitiesMap.keySet()) {
            CapabilityType capabilityType = m_objectFactory.createCapabilityType();
            capabilityType.setName(key);
            capabilityType.setMode(capabilitiesMap.get(key));
            capabilities.getCapability().add(capabilityType);
        }
        granteeCapabilities.setCapabilities(capabilities);
        return granteeCapabilities;
    }

    public ProjectListType invokeQueryProjects(TableauCredentialsType credential, String siteId) {
        m_logger.info(String.format("Querying projects on site '%s'.", siteId));
        String url = Operation.QUERY_PROJECTS.getUrl(siteId);
        TsResponse response = get(url, credential.getToken());
        if (response.getProjects() != null) {
            m_logger.info("Query projects is successful!");
            return response.getProjects();
        }
        return null;
    }

    public TableauCredentialsType invokeSignIn(String username, String password, String contentUrl) {
        m_logger.info("Signing in to Tableau Server");
        String url = Operation.SIGN_IN.getUrl();
        TsRequest payload = createPayloadForSignin(username, password, contentUrl);
        TsResponse response = post(url, null, payload);
        if (response.getCredentials() != null) {
            m_logger.info("Sign in is successful!");
            return response.getCredentials();
        }
        return null;
    }

    public void invokeSignOut(TableauCredentialsType credential) {
        m_logger.info("Signing out of Tableau Server");
        String url = Operation.SIGN_OUT.getUrl();
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse clientResponse = webResource.header(TABLEAU_AUTH_HEADER, credential.getToken()).post(
                ClientResponse.class);
        if (clientResponse.getStatus() == Status.NO_CONTENT.getStatusCode()) {
            m_logger.info("Successfully signed out of Tableau Server");
        } else {
            m_logger.error("Failed to sign out of Tableau Server");
        }
    }

    private TsRequest createPayloadForSignin(String username, String password, String contentUrl) {
        TsRequest requestPayload = m_objectFactory.createTsRequest();
        TableauCredentialsType signInCredentials = m_objectFactory.createTableauCredentialsType();
        SiteType site = m_objectFactory.createSiteType();
        site.setContentUrl(contentUrl);
        signInCredentials.setSite(site);
        signInCredentials.setName(username);
        signInCredentials.setPassword(password);
        requestPayload.setCredentials(signInCredentials);
        return requestPayload;
    }

    private TsResponse get(String url, String authToken) {
        Client client = Client.create();
        WebResource webResource = client.resource(url).queryParam("pageSize", "1000").queryParam("pageNumber", "1");
        ClientResponse clientResponse = webResource.header(TABLEAU_AUTH_HEADER, authToken).get(ClientResponse.class);
        String responseXML = clientResponse.getEntity(String.class);
        m_logger.info("Response: \n" + responseXML);
        return unmarshalResponse(responseXML);
    }

    private void invokeAppendFileUpload(TableauCredentialsType credential, String uploadSessionId,
            byte[] chunk) {
        m_logger.info(String.format("Appending to file upload '%s'.", uploadSessionId));
        String url = Operation.APPEND_FILE_UPLOAD.getUrl(credential.getSite().getId(), uploadSessionId);
        try (FileOutputStream outputStream = new FileOutputStream("appendFileUpload.temp")) {
            outputStream.write(chunk);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create temporary file to append to file upload");
        }
        BodyPart filePart = new FileDataBodyPart("tableau_file", new File("appendFileUpload.temp"),
                MediaType.APPLICATION_OCTET_STREAM_TYPE);
        putMultipart(url, credential.getToken(), null, filePart);
    }

    private FileUploadType invokeInitiateFileUpload(TableauCredentialsType credential) {
        m_logger.info(String.format("Initia projects on site '%s'.", credential.getSite().getId()));
        String url = Operation.INITIATE_FILE_UPLOAD.getUrl(credential.getSite().getId());
        TsResponse response = post(url, credential.getToken());
        if (response.getFileUpload() != null) {
            m_logger.info("Initiate file upload is successful!");
            return response.getFileUpload();
        }
        return null;
    }

    private TsResponse post(String url, String authToken) {
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse clientResponse = webResource.header(TABLEAU_AUTH_HEADER, authToken).post(ClientResponse.class);
        String responseXML = clientResponse.getEntity(String.class);
        m_logger.debug("Response: \n" + responseXML);
        return unmarshalResponse(responseXML);
    }

    private TsResponse post(String url, String authToken, TsRequest requestPayload) {
        StringWriter writer = new StringWriter();
        if (requestPayload != null) {
            try {
                s_jaxbMarshaller.marshal(requestPayload, writer);
            } catch (JAXBException ex) {
                ex.printStackTrace();
            }
        }
        String payload = writer.toString();
        m_logger.debug("Input payload: \n" + payload);
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse clientResponse = webResource.header(TABLEAU_AUTH_HEADER, authToken)
                .type(MediaType.TEXT_XML_TYPE).post(ClientResponse.class, payload);
        String responseXML = clientResponse.getEntity(String.class);
        m_logger.debug("Response: \n" + responseXML);
        return unmarshalResponse(responseXML);
    }

    private TsResponse postMultipart(String url, String authToken, TsRequest requestPayload, BodyPart filePart) {
        StringWriter writer = new StringWriter();
        String closeTag = "";
        if (requestPayload != null) {
            try {
                s_jaxbMarshaller.marshal(requestPayload, writer);
            } catch (JAXBException ex) {
                closeTag = "</datasource></tsRequest>";
            }
        }
        String payload = writer.toString()+ closeTag;
        m_logger.debug("Input payload: \n" + payload);
        BodyPart payloadPart = new FormDataBodyPart(TABLEAU_PAYLOAD_NAME, payload);
        MultiPart multipart = new MultiPart();
        multipart.bodyPart(payloadPart);

        if(filePart != null) {
            multipart.bodyPart(filePart);
        }
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse clientResponse = webResource.header(TABLEAU_AUTH_HEADER, authToken)
                .type(MultiPartMediaTypes.createMixed()).post(ClientResponse.class, multipart);
        String responseXML = clientResponse.getEntity(String.class);
        m_logger.debug("Response: \n" + responseXML);
        return unmarshalResponse(responseXML);
    }

    private TsResponse putMultipart(String url, String authToken, TsRequest requestPayload, BodyPart filePart) {
        StringWriter writer = new StringWriter();
        if (requestPayload != null) {
            try {
                s_jaxbMarshaller.marshal(requestPayload, writer);
            } catch (JAXBException ex) {
                m_logger.error("There was a problem marshalling the payload");
            }
        }
        String payload = writer.toString();
        m_logger.debug("Input payload: \n" + payload);
        BodyPart payloadPart = new FormDataBodyPart(TABLEAU_PAYLOAD_NAME, payload);
        MultiPart multipart = new MultiPart();
        multipart.bodyPart(payloadPart);
        if(filePart != null) {
            multipart.bodyPart(filePart);
        }
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse clientResponse = webResource.header(TABLEAU_AUTH_HEADER, authToken)
                .type(MultiPartMediaTypes.createMixed()).put(ClientResponse.class, multipart);
        String responseXML = clientResponse.getEntity(String.class);
        m_logger.debug("Response: \n" + responseXML);
        return unmarshalResponse(responseXML);
    }

    private TsResponse unmarshalResponse(String responseXML) {
        TsResponse tsResponse = m_objectFactory.createTsResponse();
        try {
            StringReader reader = new StringReader(responseXML);
            tsResponse = s_jaxbUnmarshaller.unmarshal(new StreamSource(reader), TsResponse.class).getValue();
        } catch (JAXBException e) {
            m_logger.error("Failed to parse response from server due to:");
            e.printStackTrace();
        }
        return tsResponse;
    }
    
    /*
     * 
     * Invoke Publish Datasource
     * 
     */
    
    private TsRequest createPayloadToPublishDatasource(String datasourceName, String projectId) {
        TsRequest requestPayload = m_objectFactory.createTsRequest();
        DataSourceType datasource = m_objectFactory.createDataSourceType();
        ProjectType project = m_objectFactory.createProjectType();
        project.setId(projectId);
        datasource.setName(datasourceName);
        datasource.setProject(project);
        requestPayload.setDatasource(datasource);
        return requestPayload;
    }
    
    public DataSourceType invokePublishDataSource(TableauCredentialsType credential,
            String projectId, String datasourceName, File datasourceFile) {
        FileUploadType fileUpload = invokeInitiateFileUpload(credential);
        UriBuilder builder = Operation.PUBLISH_DATASOURCE.getUriBuilder()
                .queryParam("uploadSessionId", fileUpload.getUploadSessionId())
                .queryParam("datasourceType", Files.getFileExtension(datasourceFile.getName()))
                .queryParam("overwrite", true);
        String url = builder.build(credential.getSite().getId()).toString();
        System.out.println("URL POST : "+url);
        byte[] buffer = new byte[100000];

        try (FileInputStream inputStream = new FileInputStream(datasourceFile.getAbsolutePath())) {
            while (inputStream.read(buffer) != -1) {
                invokeAppendFileUpload(credential, fileUpload.getUploadSessionId(), buffer);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read the datasource file.");
        }
        TsRequest payload = createPayloadToPublishDatasource(datasourceName, projectId);
        TsResponse response = postMultipart(url, credential.getToken(), payload, null);
        if (response.getDatasource() != null) {
            return response.getDatasource();
        }
        return null;
    }
}
