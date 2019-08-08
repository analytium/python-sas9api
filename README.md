# python-sas9api

This script allows the user to connect to the SAS server and 
to get information about data and manage it.

This script requires that `requests` module be installed within 
the Python environment you are using this script in.

This file can also be imported as a module and contains the following functions for connecting to the SAS server:

    * get_metadata_server_config - returns the current metadata server configuration
    * get_license_info - returns the information about active SAS Proxy license
    * get_workspace_server_list - returns the list of available workspace servers and their 
        connections from the metadata server
    * get_workspace_server_config - returns the workspace server information and its connections 
        from the metadata server by name
    * get_stp_server_list - returns the list of available Stored Process Servers and their 
        connections from the metadata server
    * get_stp_server_config - returns the Stored Process Server information and its connections 
        from the metadata server by name
    * execute_command - sends a SAS command for execution to the workspace server
    * get_user_list - returns the list of server users and their identities
    * get_configured_user_info - returns the configured user information and its identities
    * get_user_info - returns server user information and its identities by name
    * get_group_list - returns the list of groups and their associated groups and users
    * get_group_info - returns group information and its associated groups and users by group name
    * get_role_list - returns the list of roles and their associated groups and users
    * get_role_info - returns role information and its associated groups and users by role name
    * get_library_list - returns the list of libraries for the workspace server
    * get_library_info - returns library information for theworkspace server by library name
    * create_library - creates a library at a given server with given library name and parameters
    * delete_library - removes all libraries with matching library name
    * get_dataset_list - returns the list of datasets for the specific library by library name
    * get_dataset_info - returns dataset information by dataset name and library name
    * retrieve_data - retrieves data from a dataset by dataset name and library name
    * insert_data - inserts data into a dataset or replaces data by a key
    * replace_all_data - replaces all data in a dataset with input data
    * delete_dataset - deletes dataset from a library
    * find_object - searches for objects
    * copy - copies an object to a folder
    * move_object - moves an object between folders
    * delete_object - deletes an object by its name and folder name
