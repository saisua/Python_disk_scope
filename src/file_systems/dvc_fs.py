import os
import dvc.api

class DVC(dvc.api.DVCFileSystem):
    def __init__(self, folder_name, **kwargs):
        super().__init__(url=folder_name)

    def open(self, file, *args, version='', source='', **kwargs):
        return self.open(f"{self.dvc_url}/{file}", *args, repo=source, rev=version, **kwargs)

    def rename(self, *args, **kwargs):
        return os.rename(*args, **kwargs)
