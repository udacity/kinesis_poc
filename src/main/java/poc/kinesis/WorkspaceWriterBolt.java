package poc.kinesis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class WorkspaceWriterBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(WorkspaceWriterBolt.class);
    ObjectMapper mapper = new ObjectMapper();
    private transient OutputCollector collector;
    private Table table;

    enum WorkspaceEvent {
        VIEWED("count_viewed", "viewed_users"),
        INTERACTED("sessions_interacted", "interacted_users", "total_time_sec", "num_interactions"),
        TERMINAL_ADDED("count_terminals_added","terminal_added_users"),
        TERMINAL_REMOVED("count_terminals_removed","terminal_removed_users"),
        CODE_PREVIEWED("count_preview_opened", "preview_opened_users"),
        SUBMIT_PROJECT_CLICKED("count_submit_click", "submit_click_users"),
        PROJECT_SUBMITTED("count_project_submitted", "project_submitted_users"),
        CODE_RESET_CLICKED("count_code_reset_click", "code_reset_click_users"),
        CODE_RESET("count_code_reset", "code_reset_users");

        private final String eventCount;
        private final String userSet;
        private final String totalTimeSec;
        private final String numInteractions;

        WorkspaceEvent(String eventCount, String userSet) {
            this(eventCount, userSet, null, null);
        }

        WorkspaceEvent(String eventCount, String userSet, String totalTimeSec, String numInteractions) {
            this.eventCount = eventCount;
            this.userSet = userSet;
            this.totalTimeSec = totalTimeSec;
            this.numInteractions = numInteractions;
        }

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        AmazonDynamoDB client = new AmazonDynamoDBClient();
        client.setEndpoint("dynamodb.us-west-2.amazonaws.com");
        DynamoDB dynamoDB = new DynamoDB(client);
        table = dynamoDB.getTable("workspace_stat");
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("Start process tuple in WorkspaceWriterBolt");
        try {
            JsonNode node = mapper.readTree(tuple.getString(0));

            String id = getId(node);
            if (id!=null) {
                String event = node.get("event").textValue();
                LOG.info("Process " + event + " event");
                switch (event) {
                    case "Workspace Viewed":
                        updateState(id, WorkspaceEvent.VIEWED, node.get("userId").textValue());
                        break;
                    case "Workspace Interacted":
                        updateInteracted(id, WorkspaceEvent.INTERACTED, node.get("userId").textValue(),
                                node.get("properties").get("workspace_session").textValue(),
                                node.get("properties").get("total_time_sec").doubleValue(),
                                node.get("properties").get("num_interactions").intValue());
                        break;
                    case "Workspace Terminal Added":
                        updateState(id, WorkspaceEvent.TERMINAL_ADDED, node.get("userId").textValue());
                        break;
                    case "Workspace Terminal Removed":
                        updateState(id, WorkspaceEvent.TERMINAL_REMOVED, node.get("userId").textValue());
                        break;
                    case "Workspace Code Previewed":
                        updateState(id, WorkspaceEvent.CODE_PREVIEWED, node.get("userId").textValue());
                        break;
                    case "Workspace Submit Project Clicked":
                        updateState(id, WorkspaceEvent.SUBMIT_PROJECT_CLICKED, node.get("userId").textValue());
                        break;
                    case "Workspace Project Submitted":
                        updateState(id, WorkspaceEvent.PROJECT_SUBMITTED, node.get("userId").textValue());
                        break;
                    case "Workspace Code Reset Clicked":
                        updateState(id, WorkspaceEvent.CODE_RESET_CLICKED, node.get("userId").textValue());
                        break;
                    case "Workspace Code Reset":
                        updateState(id, WorkspaceEvent.CODE_RESET, node.get("userId").textValue());
                        break;
                    //case "Workspace Files Uploaded":
                    //    break;
                    default:
                        LOG.warn("Unknown type of workspace event received " + event);
                }
            }
        }catch (IOException ex){
            LOG.error("Read data failed", ex);
        }

        collector.ack(tuple);
    }

    private void updateInteracted(String id, WorkspaceEvent event, String userId, String sessionId, double totalTimeSec, int numInteractions) {
        Map<String, String> expressionAttributeNames = new HashMap<String, String>();
        expressionAttributeNames.put("#sessionSet", event.eventCount);
        expressionAttributeNames.put("#userSet", event.userSet);
        expressionAttributeNames.put("#interacted", "workspace_interacted");
        expressionAttributeNames.put("#sessionId", sessionId);
        expressionAttributeNames.put("#totalTimeSec", event.totalTimeSec);
        expressionAttributeNames.put("#numInteractions", event.numInteractions);

        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":sessionId", new HashSet<String>(Arrays.asList(sessionId)));
        expressionAttributeValues.put(":userId", new HashSet<String>(Arrays.asList(userId)));
        expressionAttributeValues.put(":totalTimeSec", totalTimeSec);
        expressionAttributeValues.put(":numInteractions", numInteractions);

        try {
            UpdateItemOutcome outcome = table.updateItem(
                    "id",          // key attribute name
                    id,           // key attribute value
                    "add #sessionSet :sessionId, #userSet :userId " +
                            "set #interacted.#sessionId.#totalTimeSec = :totalTimeSec, #interacted.#sessionId.#numInteractions = :numInteractions", // UpdateExpression
                    expressionAttributeNames,
                    expressionAttributeValues);
            LOG.debug("update outcome: " + outcome.toString());
        }catch (AmazonServiceException ex){
            UpdateItemOutcome outcome = table.updateItem(
                    "id",          // key attribute name
                    id,           // key attribute value
                    "add #sessionSet :sessionId, #userSet :userId " +
                            "set #interacted = :empty, set #interacted.#sessionId = :empty, set #interacted.#sessionId.#totalTimeSec = :totalTimeSec, #interacted.#sessionId.#numInteractions = :numInteractions", // UpdateExpression
                    expressionAttributeNames,
                    expressionAttributeValues);
            LOG.debug("update outcome: " + outcome.toString());
        }

    }

    private void updateState(String id, WorkspaceEvent event, String uid){
        Map<String, String> expressionAttributeNames = new HashMap<String, String>();
        expressionAttributeNames.put("#eventCount", event.eventCount);
        expressionAttributeNames.put("#userSet", event.userSet);

        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":val1", 1);
        expressionAttributeValues.put(":val2", new HashSet<String>(Arrays.asList(uid)));

        try {
            UpdateItemOutcome outcome = table.updateItem(
                    "id",          // key attribute name
                    id,           // key attribute value
                    "set #eventCount = #eventCount + :val1 add #userSet :val2 ", // UpdateExpression
                    "attribute_exists(#eventCount) ",
                    expressionAttributeNames,
                    expressionAttributeValues);
            LOG.debug("update outcome: " + outcome.toString());
        } catch (ConditionalCheckFailedException ex) {
            UpdateItemOutcome outcome = table.updateItem(
                    "id",          // key attribute name
                    id,           // key attribute value
                    "set #eventCount = :val1 add #userSet :val2 ", // UpdateExpression
                    expressionAttributeNames,
                    expressionAttributeValues);
            LOG.debug("update outcome: " + outcome.toString());
        }
    }

    private String getId(JsonNode node){
        try {
            String day_of = node.get("receivedAt").textValue().substring(0, 11);

            String nd_key = node.get("properties").get("nd_key").textValue();
            String nd_version = node.get("properties").get("nd_version").textValue();
            String nd_locale = node.get("properties").get("nd_locale").textValue();
            String workspace_id = node.get("properties").get("workspace_id").textValue();
            String concept_key = node.get("properties").get("concept_key").textValue();
            if (day_of.isEmpty() || nd_key.isEmpty() || nd_version.isEmpty() || nd_locale.isEmpty() || workspace_id.isEmpty() || concept_key.isEmpty()) {
                LOG.error("missing required field to construct primary key. " + node.toString());
                return null;
            }
            return (new StringBuilder().append(day_of + "_").append(nd_key + "_").append(nd_version + "_").append(nd_locale + "_").append(workspace_id + "_").append(concept_key)).toString();

        }catch (Exception ex){
            LOG.error("missing required field to construct primary key. " + node.toString());
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
