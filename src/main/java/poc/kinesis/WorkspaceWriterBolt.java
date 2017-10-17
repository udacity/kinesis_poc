package poc.kinesis;

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
        INTERACTED("sessions_interacted", "interacted_users"),
        TERMINAL_ADDED("count_terminals_added","terminal_added_users"),
        TERMINAL_REMOVED("count_terminals_removed","terminal_removed_users"),
        CODE_PREVIEWED("count_preview_opened", "preview_opened_users"),
        SUBMIT_PROJECT_CLICKED("count_submit_click", "submit_click_users"),
        PROJECT_SUBMITTED("count_project_submitted", "project_submitted_users"),
        CODE_RESET_CLICKED("count_code_reset_click", "code_reset_click_users"),
        CODE_RESET("count_code_reset", "code_reset_users");

        private final String eventCount;
        private final String userSet;
        WorkspaceEvent(String eventCount, String userSet) {
            this.eventCount = eventCount;
            this.userSet = userSet;
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
                        updateState(id, WorkspaceEvent.INTERACTED, node.get("userId").textValue());
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

    void updateState(String id, WorkspaceEvent event, String uid){
        LOG.warn("db update start process");
        Map<String, String> expressionAttributeNames = new HashMap<String, String>();
        expressionAttributeNames.put("#A", event.eventCount);
        expressionAttributeNames.put("#B", event.userSet);

        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":val1", 1);
        expressionAttributeValues.put(":val2", new HashSet<String>(Arrays.asList(uid)));

        try {
            UpdateItemOutcome outcome = table.updateItem(
                    "id",          // key attribute name
                    id,           // key attribute value
                    "set #A = #A + :val1 add #B :val2 ", // UpdateExpression
                    "attribute_exists(#A) ",
                    expressionAttributeNames,
                    expressionAttributeValues);
            LOG.warn("db response1: " + outcome.toString());
        } catch (ConditionalCheckFailedException ex) {
            UpdateItemOutcome outcome = table.updateItem(
                    "id",          // key attribute name
                    id,           // key attribute value
                    "set #A = :val1 add #B :val2 ", // UpdateExpression
                    expressionAttributeNames,
                    expressionAttributeValues);
            LOG.warn("db response2: " + outcome.toString());
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
            return (new StringBuilder().append(day_of + "_").append(nd_key + "_").append(nd_version + "_").append(nd_locale + "_").append(workspace_id + "_").append(concept_key + "_")).toString();

        }catch (Exception ex){
            LOG.error("missing required field to construct primary key. " + node.toString());
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
