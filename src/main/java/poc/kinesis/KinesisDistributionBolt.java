/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package poc.kinesis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class KinesisDistributionBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(KinesisDistributionBolt.class);
    private transient OutputCollector collector;
    ObjectMapper mapper = new ObjectMapper();
    private static final String EVENT = "event";
    private static final String PROPERTIES = "properties";
    private static final String WORKSPACE = "Workspace";

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String jsonString = (String)input.getValues().get(2);
        try {
            JsonNode node = mapper.readTree(jsonString);

            if(node.has(EVENT) && node.get(EVENT).textValue().startsWith(WORKSPACE)){
                //emit to downstream
                collector.emit(new Values(jsonString));
            }else{
                //drop unknow event
            }
            collector.ack(input);
        } catch (IOException ex) {
            LOG.error("process record failed for "+jsonString, ex);
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("workspace_event"));
    }
}
