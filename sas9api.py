"""SAS9API

This script allows the user to connect to the SAS server and 
to get information about data and manage it.

This script requires that `requests` module be installed within 
the Python environment you are using this script in.

This file can also be imported as a module and contains the following
functions:

    * assemble_url - an auxiliary function which return the url based on the endpoint
    * make_request - makes a request and returns a response from the server
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
"""


import requests
from requests.exceptions import HTTPError


def assemble_url(url, endpoint):
    """This is an auxiliary function. It checks whether '/' is provided by the user at the end of the url.
       If '/' is not provided it adds it and returns url based on the endpoint.
       
    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    endpoint : str
        Endpoint.

    Returns
    -------
    str
        URL ready to be used to make requests.
    """

    
    if url.strip().endswith("/"):
        url = f"{url}{endpoint}"
    else:
        url = f"{url}/{endpoint}"
    return url


def make_request(method, url, initial_params={}, data="", json_data=[], only_payload=False):
    """Makes HTTP requests.
       
    Parameters
    ----------
    method : str
        Request method.
    url : str
        The URL of the request.
    initial_params : dict, optional
        Parameters of the request (default is an Empty dictionary)
    data : str, optional
        Data to be submitted with the request as a string (default is an empty string).
    json_data : list, optional
        Data to be submitted with the request as json data(default is an empty list).
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        False). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
        
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """
    

    try:
        response = requests.request(method, url=url, params=initial_params, data=data, json=json_data)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        print(response.json()['error'])
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        print('Success!')
        if only_payload:
            return response.json()["payload"]
        else:
            return response.json()


def get_metadata_server_config(url, only_payload=False):
    """Gets the current metadata server configuration.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        False). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.

    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """
    
    
    endpoint = "sas/"
    
    return make_request("GET", assemble_url(url, endpoint), only_payload=only_payload)


def get_license_info(url, only_payload=False):
    """Gets the information about active SAS Proxy license.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        False). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.

    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """
    

    endpoint = "sas/license"
    
    return make_request("GET", assemble_url(url, endpoint), only_payload=only_payload)


def get_workspace_server_list(url, repository_name="Foundation", only_payload=False):
    """Gets the list of available workspace servers and their connections from the metadata server.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.

    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    endpoint = "sas/servers"
    
    initial_params = {"repositoryName": repository_name}
  
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_workspace_server_config(url, server_name, repository_name="Foundation", only_payload=False):
    """Gets the workspace server information and its connections from the metadata server by a server name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    server_name : str
        Server name.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """
    

    endpoint = f"sas/servers/{server_name}"
    
    initial_params = {"repositoryName": repository_name}
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_stp_server_list(url, repository_name="Foundation", only_payload=False):
    """Gets the list of available Stored Process Servers and their connections from the metadata server.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    endpoint = "sas/stp"
    
    initial_params = {"repositoryName": repository_name}
   
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_stp_server_config(url, server_name, repository_name="Foundation", only_payload=False):
    """Gets the Stored Process Server information and its connections from the metadata server by a server name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    server_name : str
        Server name.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """

    
    endpoint = f"sas/stp/{server_name}"
    
    initial_params = {"repositoryName": repository_name}
        
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def execute_command(url, command, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, log_enabled=True, only_payload=False):
    """Sends a SAS command for execution to the workspace server.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API
    command : str
        SAS command to send to the server.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None). 
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None). 
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    log_enabled : bool
        A flag which enables log output in the endpoint response (default is True); doesn't populate response fields
        if set to False.
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
        
    Example
    -------
        >>> execute_command(url, "proc print data=sashelp.class;run;")
    """

    
    if log_enabled:
        initial_params = {"logEnabled": "true"}
    else:
        initial_params = {"logEnabled": "false"}
        
    endpoint = "sas/cmd"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/cmd"
        initial_params["repositoryName"] = repository_name    
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
    
    return make_request("PUT", assemble_url(url, endpoint),
                       initial_params=initial_params, data=command, only_payload=only_payload)


# USERS *********************************************************************************************************
def get_user_list(url, repository_name="Foundation", only_payload=False):
    """Gets the list of server users and their identities.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    endpoint = "sas/meta/users"
    
    initial_params = {"repositoryName": repository_name}
        
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_configured_user_info(url, repository_name="Foundation", only_payload=False):
    """Gets the configured user information and its identities.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """
 

    endpoint = "sas/user"
    
    initial_params = {"repositoryName": repository_name}
        
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)

 
def get_user_info(url, user_name, repository_name="Foundation", only_payload=False):
    """Gets the server user information and its identities by a user name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    user_name : str
        User name.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """

    
    endpoint = f"sas/meta/users/{user_name}"
    
    initial_params = {"repositoryName": repository_name}
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)
        

def get_group_list(url, repository_name="Foundation", only_payload=False):
    """Gets the list of groups and their associated groups and users.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    endpoint = "sas/meta/groups"
        
    initial_params = {"repositoryName": repository_name}
        
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_group_info(url, group_name, repository_name="Foundation", only_payload=False):
    """Gets the group information and its associated groups and users by a group name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    group_name : str
        Group name.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """

    
    endpoint = f"sas/meta/groups/{group_name}"
        
    initial_params = {"repositoryName": repository_name}
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_role_list(url, repository_name="Foundation", only_payload=False):
    """Gets the list of roles and their associated groups and users.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    endpoint = "sas/meta/roles"
        
    initial_params = {"repositoryName":repository_name}
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_role_info(url, role_name, repository_name="Foundation", only_payload=False):
    """Gets the role information and its associated groups and users by a role name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    role_name : str
        Role name.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """
 

    endpoint = f"sas/meta/roles/{role_name}"
        
    initial_params = {"repositoryName": repository_name}
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


# LIBRARIES *********************************************************************************************************
def get_library_list(url, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, only_payload=False):
    """Gets the list of libraries for the workspace server.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    initial_params = dict()
    
    endpoint = "sas/libraries"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries"
        initial_params["repositoryName"] = repository_name
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_library_info(url, library_name, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, only_payload=False):
    """Gets the library information for the workspace server by a library name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
        
    Example
    -------
        >>> get_library_info(url, "sashelp", server_name="SASApp")
        {'status': 200,
         'error': None,
         'payload': {'id': None,
         'libname': 'SASHELP',
         'engine': 'V9',
         'path': 'your_path',
         'level': 1,
         'readonly': False,
         'sequential': False,
         'temp': False}}
    """

    
    initial_params = dict()
    
    endpoint = f"sas/libraries/{library_name}"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}"
        initial_params["repositoryName"] = repository_name
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def create_library(url, server_name, library_name, engine, display_name, path, location, 
                   repository_name="Foundation", is_preassigned=False, only_payload=False):
    """Creates a library at a given server with given library name and parameters.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    server_name : str
        Workspace server name.
    library_name : str
        Created library name as will be used in a LIBNAME statement.
    engine : str
        LIBNAME engine.
    display_name : str
        Library display name.
    path : str
        Library data path.
    location : str
        Folder to place metadata object in.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    is_preassigned : bool
        A flag whic defines whether the created library should be preassigned (default is False).  
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """

    
    endpoint = f"sas/servers/{server_name}/libraries/{library_name}"
    
    initial_params = {"engine": engine, "displayName": display_name, "path": path, 
                      "location": location, "repositoryName": repository_name}
    if is_preassigned:
        initial_params["isPreassigned"] = "true"
    else:
        initial_params["isPreassigned"] = "false"
    
    return make_request("POST", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def delete_library(url, server_name, library_name, repository_name="Foundation", only_payload=False):
    """Removes all libraries with matching library name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    server_name : str
        Workspace server name.
    library_name : str
        Library name.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/bool
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a boolean (truncated).
    """

    
    endpoint = f"sas/servers/{server_name}/libraries/{library_name}"
    
    initial_params = {"repositoryName": repository_name}
    
    return make_request("DELETE", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


# DATASETS *********************************************************************************************************
def get_dataset_list(url, library_name, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, only_payload=False):
    """Gets the list of datasets for the specific library by a library name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
        
    Example
    -------
        >>> get_dataset_list(url, "sashelp", server_name="SASApp", only_payload=True)
        [{'name': 'AACOMP',
          'type': 'DATA',
          'label': '',
          'creationDate': '2018-11-01T01:15:06.18',
          'modificationDate': '2018-11-01T01:15:06.18',
          'objectsNumber': 2020,
          'columns': None},
         {'name': 'AARFM',
          'type': 'DATA',
          'label': '',
          'creationDate': '2018-10-25T02:06:00.634',
          'modificationDate': '2018-10-25T02:06:00.634',
          'objectsNumber': 130,
          'columns': None},
         {'name': 'ADSMSG',
          'type': 'MSGFILE',
          'label': '',
          'creationDate': '2018-10-25T02:13:18.751',
          'modificationDate': '2018-10-25T02:13:18.751',
          'objectsNumber': 426,
          'columns': None},
          ...]
    """

    
    initial_params = dict()
    
    endpoint = f"sas/libraries/{library_name}/datasets"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}/datasets"
        initial_params = {"repositoryName": repository_name}
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def get_dataset_info(url, library_name, dataset_name, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, only_payload=False):
    """Gets the dataset information by a dataset name and a library name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    dataset_name : str
        Dataset name.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
        
    Example
    -------
        >>> get_dataset_info(url, "sashelp", "class", server_name="SASApp", only_payload=True)
        {'name': 'CLASS',
         'type': 'DATA',
         'label': 'Student Data',
         'creationDate': '2018-10-25T02:06:04.07',
         'modificationDate': '2018-10-25T02:06:04.07',
         'objectsNumber': 19,
         'columns': [{'name': 'Name',
           'type': 'char',
           'extendedType': 'char',
           'length': 8,
           'notNull': False,
           'indexType': '',
           'sortedBy': 0,
           'columnNumber': 1,
           'label': ''},
          {'name': 'Sex',
           'type': 'char',
           'extendedType': 'char',
           'length': 1,
           'notNull': False,
           'indexType': '',
           'sortedBy': 0,
           'columnNumber': 2,
           'label': ''},
          {'name': 'Age',
           'type': 'num',
           'extendedType': 'num',
           'length': 8,
           'notNull': False,
           'indexType': '',
           'sortedBy': 0,
           'columnNumber': 3,
           'label': ''},
          {'name': 'Height',
           'type': 'num',
           'extendedType': 'num',
           'length': 8,
           'notNull': False,
           'indexType': '',
           'sortedBy': 0,
           'columnNumber': 4,
           'label': ''},
          {'name': 'Weight',
           'type': 'num',
           'extendedType': 'num',
           'length': 8,
           'notNull': False,
           'indexType': '',
           'sortedBy': 0,
           'columnNumber': 5,
           'label': ''}]}
    """

    
    initial_params = dict()
    
    endpoint = f"sas/libraries/{library_name}/datasets/{dataset_name}"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}/datasets/{dataset_name}"
        initial_params = {"repositoryName": repository_name}
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)

        
def retrieve_data(url, library_name, dataset_name, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, limit=100, offset=0, filter_=None, only_payload=False):
    """Retrieves data from the dataset by a dataset name and a library name.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    dataset_name : str
        Dataset name.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    limit : int
        Number of records to retrieve (default is 100, maximum value is 10000).
    offset : int
        Dataset record offset (default is 0).
    filter_ : string    
        Dataset filter (JSON). Default is None.
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list of records (truncated).
        
    Examples
    -------
        >>> retrieve_data(url, "sashelp", "buy", server_name="SASApp", only_payload=True)
        [{'AMOUNT': -110000.0, 'DATE': '1996-01-01'},
         {'AMOUNT': -1000.0, 'DATE': '1997-01-01'},
         {'AMOUNT': -1000.0, 'DATE': '1998-01-01'},
         {'AMOUNT': -51000.0, 'DATE': '1999-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2000-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2001-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2002-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2003-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2004-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2005-01-01'},
         {'AMOUNT': 48000.0, 'DATE': '2006-01-01'}]
         
        >>> retrieve_data(url, "sashelp", "buy", server_name="SASApp", filter_='{"AMOUNT":-2000}', only_payload=True)
        [{'AMOUNT': -2000.0, 'DATE': '2000-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2001-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2002-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2003-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2004-01-01'},
         {'AMOUNT': -2000.0, 'DATE': '2005-01-01'}]
    """

    
    initial_params = {"limit": limit, "offset": offset}
    
    endpoint = f"sas/libraries/{library_name}/datasets/{dataset_name}/data"
    
    if filter_ is not None:
        initial_params["filter"] = filter_
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}/datasets/{dataset_name}/data"
        initial_params["repositoryName"] = repository_name
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
        
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)
    
        
def insert_data(url, library_name, dataset_name, json_data, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, by_key=None, only_payload=False):
    """Inserts data into the dataset or replaces data by a key.
       The dataset column name ('by_key') is used to update all records with the 'by_key' value in this column.

    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    dataset_name : str
        Dataset name.
    json_data : list
        Data to insert (see an example below).
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    by_key : str
        Dataset key for record matching (default is None).
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
        
    Example
    -------
        >>> insert_data(url, "mylib", "class", [{"Name": "Bill", "Sex": "M"}, {"Name":"John", "Sex": "M"}], 
                        server_name="SASApp", only_payload=True)
        {'status': 200,
         'error': None,
         'payload': {'itemsInserted': 2, 'itemsRemoved': 0, 'itemsUpdated': None}}
    """

    
    initial_params = dict()
    if by_key is not None:
        initial_params["byKey"] = by_key
        
    endpoint = f"sas/libraries/{library_name}/datasets/{dataset_name}/data"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}/datasets/{dataset_name}/data"
        initial_params["repositoryName"] = repository_name
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
    
    return make_request("PUT", assemble_url(url, endpoint),
                       initial_params=initial_params, 
                       json_data=json_data, only_payload=only_payload)

        
def replace_all_data(url, library_name, dataset_name, json_data, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, only_payload=False):
    """Replaces all data in the dataset with input data.
       
    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    dataset_name : str
        Dataset name.
    json_data : list
        Data to replace (see an example below).
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
        
    Example
    -------
        >>> replace_all_data(url, "mylib", "class", [{"Name":"A"},{"Age":45}], server_name="SASApp", only_payload=False)
        
        {'status': 200,
         'error': None,
         'payload': {'itemsInserted': 2, 'itemsRemoved': 21, 'itemsUpdated': None}}
    """

    
    initial_params = dict()
    
    endpoint = f"sas/libraries/{library_name}/datasets/{dataset_name}/data"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}/datasets/{dataset_name}/data"
        initial_params["repositoryName"] = repository_name
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
        
    return make_request("POST", assemble_url(url, endpoint),
                       initial_params=initial_params,  
                       json_data=json_data, only_payload=only_payload)


def delete_dataset(url, library_name, dataset_name, server_name=None, repository_name="Foundation", 
                    server_url=None, server_port=None, only_payload=False):
    """Deletes dataset from a library.
       
    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    library_name : str
        Library name.
    dataset_name : str
        Dataset name.
    server_name : str (optional, the default Server Name from the configuration file will be used 
                       if neither 'server_name nor ('server_url' and 'server_port') are specified)
        Workspace server name (default is None).
    repository_name : str, optional
        Repository name (default is 'Foundation').
    server_url : str (optional; must come in pair with 'server_port' if specified)
        Workspace server URL (default is None).
    server_port : int/str (optional; must come in pair with 'server_url' if specified)
        Workspace server port (default is None).
        
    If 'server_name' and the pair ('server_url' and 'server_port') are both specified
    the request is made using 'server_name'.
    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """

    
    initial_params = dict()
    
    endpoint = f"sas/libraries/{library_name}/datasets/{dataset_name}/data"
    
    if server_name is not None:
        endpoint = f"sas/servers/{server_name}/libraries/{library_name}/datasets/{dataset_name}/data"
        initial_params["repositoryName"] = repository_name
    elif server_url is not None and server_port is not None:
        initial_params["serverUrl"] = server_url
        initial_params["serverPort"] = server_port
    else:
        print("The default Server Name from the configuration file will be used "
              "because neither 'server_name' nor ('server_url' and 'server_port') are specified!")
        
    return make_request("DELETE", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


# OBJECTS *********************************************************************************************************
def find_object(url, repository_name="Foundation", location=None, location_recursive=True, object_id=None,
                object_type=None, public_type=None, name_equals=None, name_starts=None, name_contains=None,
                name_regex=None, description_contains=None, description_regex=None, created_gt=None,
                created_lt=None, modified_gt=None, modified_lt=None, table_libref=None, table_dbms=None,
                include_associations=False, include_permissions=False, only_payload=False):
    """Finds objects.
       
    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    location : str, optional
        Folder location to search in (default is None).
    location_recursive : bool, optional
        A flag defining whether to search in subfolders or not (default is True).
    object_id : str, optional
        SAS Metadata object ID (default is None).
    object_type : str, optional
        SAS Metadata object type (default is None). Omit for any object types
    public_type : str, optional
        Comma-separated list of target PublicType attributes.
    name_equals : str, optional (if at least one other search criteria is specified)
        Search criteria: name matches (case-insensitive). Default is None.
    name_starts : str, optional (if at least one other search criteria is specified)
        Search criteria: name starts with (case-insensitive). Default is None.
    name_contains : str, optional (if at least one other search criteria is specified)
        Search criteria: name contains (case-insensitive). Default is None.
    name_regex : str, optional (if at least one other search criteria is specified)
        Search criteria: name matches regex. Default is None.
    description_contains : str, optional (if at least one other search criteria is specified)
        Search criteria: description contains (case-insensitive). Default is None.
    description_regex : str, optional (if at least one other search criteria is specified)
        Search criteria: description matches regex. Default is None.
    created_gt : str, optional (if at least one other search criteria is specified)
        Search criteria: MetadataCreated greater than (ISO datatime format). Default is None.
    created_lt : str, optional (if at least one other search criteria is specified)
        Search criteria: MetadataCreated lower than (ISO datatime format). Default is None.
    modified_gt : str, optional (if at least one other search criteria is specified)
        Search criteria: MetadataModified greater than (ISO datatime format). Default is None.
    modified_lt : str, optional (if at least one other search criteria is specified)
        Search criteria: MetadataModified lower than (ISO datatime format). Default is None.
    table_libref : str, optional (if at least one other search criteria is specified)
        Search criteria: libref name for associated library object for a table. For table types only. Default is None.
    table_dbms : str, optional (if at least one other search criteria is specified)
        Search criteria: DBMS engine name for associated library object for a table. For table types only. Default is None.
        
    Note: at least one search criteria must be specified.
    
    include_associations : bool, optional
        A flag defining whether to include object associations in the search. Default is False.
    include_permissions : bool, optional
        A flag defining whether to include metadata object permissions in the search. Default is False.
    repository_name : str, optional
        Repository name (default is 'Foundation').
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict/list
        The server response (either full or truncated depending on the 'only_payload' flag) 
        as a dictionary (full) or a list (truncated).
    """

    
    endpoint = "sas/meta/search"
    
    initial_params = {"repositoryName": repository_name, "location": location, 
                      "locationRecursive": location_recursive, "objectID": object_id, 
                      "objectType": object_type, "publicType": public_type,
                      "nameEquals": name_equals, "nameStarts": name_starts, 
                      "nameContains": name_contains, "nameRegex": name_regex, 
                      "descriptionContains": description_contains, "descriptionRegex": description_regex,
                      "createdGt": created_gt, "createdLt": created_lt, "modifiedGt": modified_gt, 
                      "modifiedLt": modified_lt, "tableLibref": table_libref, "tableDBMS": table_dbms, 
                      "includeAssociations": include_associations, "includePermissions": include_permissions}
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)
    
"""
def copy(url, sourceLocation, sourceName, destinationLocation, publicType="", **kwargs):
    #
    #    This method will copy object to folder.
    #    This method will return a response as a dictionary.
    #
    endpoint = f"sas/meta/objects/copy"
    
    initial_params = {"sourceLocation":sourceLocation, "sourceName":sourceName, "destinationLocation":destinationLocation, 
              "publicType":publicType}
    valid_params = ["repositoryName"]
    
    return make_request("GET", assemble_url(url, endpoint),
                       initial_params=initial_params, valid_params=valid_params, kwargs=kwargs, onlyPayload=onlyPayload)
"""
    

def move_object(url, source_location, source_name, public_type, destination_location,  
                repository_name="Foundation", only_payload=False):
    """Moves object between folders.
       
    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    source_location : str
        Source folder location.
    source_name : str
        Source object name.
    public_type : str
        Source object PublicType.
    destination_location : str
        Destination folder location.
    repository_name : str, optional
        Repository name (default is 'Foundation').    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """

    # When success response is None!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    endpoint = "sas/meta/objects/move"
    
    # Error in the source code!!! Correcting it here, but when it is corrected in Java code,
    # will have to flip destinationLocation and PublicType !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    initial_params = {"sourceLocation": source_location, "sourceName": source_name, 
                      "destinationLocation": public_type, "publicType": destination_location,
                      "repositoryName": repository_name}
    
    return make_request("POST", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)


def delete_object(url, source_location, source_name, public_type, repository_name="Foundation", only_payload=False):
    """Deletes object by its name and location.
       
    Parameters
    ----------
    url : str
        The URL of the server with the installed SAS9API.
    source_location : str
        Folder location.
    source_name : str
        Object name.
    public_type : str
        Object PublicType.
    repository_name : str, optional
        Repository name (default is 'Foundation').    
    only_payload : bool, optional
        A flag used to determine the content of the response returned by the function (default is
        True). If True - the function will return the truncated server response containing only
        the payload. If False - the function will return the full response from the server.
 
    Returns
    -------
    dict
        The server response (either full or truncated depending on the 'only_payload' flag) as a dictionary.
    """
    
    # When success response is None!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    endpoint = "sas/meta/objects/delete"
        
    initial_params = {"sourceLocation": source_location, "sourceName": source_name,  
                      "publicType": public_type, "repositoryName": repository_name}
    
    return make_request("POST", assemble_url(url, endpoint),
                       initial_params=initial_params, only_payload=only_payload)