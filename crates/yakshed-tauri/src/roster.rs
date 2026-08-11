macro_rules! command_roster {
    ($apply:ident) => {
        $apply!(
            create_project,
            create_work_item,
            list_work_items,
            get_work_item_snapshot,
            get_work_item_snapshot_page,
            get_work_item_timeline_page,
            get_work_item_timeline_page_at_revision,
            get_run_approval_page,
            get_pending_user_input_page,
            start_run,
            steer_run,
            interrupt_run,
            reconcile_run,
            resolve_approval,
            respond_user_input,
            connection_put,
            connection_get,
            list_connections,
            set_connection_credential,
            list_artifacts,
            open_artifact,
            clear_cache,
        )
    };
}
