
from office365.sharepoint.files.file_creation_information import FileCreationInformation
import os
from office365.sharepoint.client_context import ClientContext


def uploadToSharepoint(input=None,output=None):

    ctx = ClientContext('https://arcbe.sharepoint.com/sites/ARC/Projects/SLA').with_user_credentials('crm_sync@arc.be','Welcome@Arc')

    DIRECTORY = 'OutputTables'
    entries = os.listdir((DIRECTORY))

    for entry in entries:
        print(entry)
        path = DIRECTORY + "/" + entry
        with open(path, 'rb') as content_file:
            file_content = content_file.read()

        file_info = FileCreationInformation()
        file_info.content = file_content
        file_info.url = os.path.basename(path)
        file_info.overwrite = True
        target_file = ctx.web.get_folder_by_server_relative_url("/Shared Documents/").files.add(file_info)
        ctx.execute_query()

