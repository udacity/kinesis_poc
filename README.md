# kinesis_poc

The POC Project read data from Kinese Stream, then aggregate data and store in DynamoDB, finally transfer data from DynamoDB to RDS.

Workspace Data Structure in DynamoDB
    
    id String (day_of+nd_key+nd_version+nd_locale+workspace_id+concept_key)
    
    interacted_users StringSet
    sessions_interacted StringSet
    workspace_interacted: {
        workspace_session_id: {
            total_time_sec double
            num_interactions int
        }
    }
    
    count_viewed int
    viewed_users StringSet
        
    count_terminals_added int
    terminal_added_users StringSet
    
    count_terminals_removed int
    terminal_removed_users StringSet
    
    count_preview_opened int
    preview_opened_users StringSet
    
    count_submit_click int
    submit_click_users StringSet
    
    count_project_submitted int
    project_submitted_users StringSet
    
    count_code_reset_click int
    code_reset_click_users StringSet
    
    count_code_reset int
    code_reset_users StringSet
    


Storm Env Setup in AWS

    sudo yum update -y
    sudo yum install -y docker
    sudo yum install -y git
    sudo service docker start
    sudo usermod -a -G docker ec2-user
    exit 
    
    docker run -d --restart always --name some-zookeeper zookeeper
    docker run -d --restart always --name some-nimbus --link some-zookeeper:zookeeper storm storm nimbus
    docker run -d --restart always --name supervisor --link some-zookeeper:zookeeper --link some-nimbus:nimbus storm storm supervisor
    docker run -d -p 8080:8080 --restart always --name ui --link some-nimbus:nimbus storm storm ui
    //Run Application
    docker run --link some-nimbus:nimbus --link some-zookeeper:zookeeper -it --rm -v $(pwd)/target/kinesis_poc-1.0-SNAPSHOT-jar-with-dependencies.jar:/topology.jar storm storm jar /topology.jar poc.kinesis.KinesisSpoutTopology LocalMode

