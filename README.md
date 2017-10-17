# kinesis_poc

Workspace Data Structure 

{
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
    
}