'''
exceptions
'''

class LostDoc(Exception):
    '''
    exceptions lost load documents
    '''
    def __init__(self, *args):
        super().__init__(*args)
    def __str__(self):
        return "no document to upload"
class DuplicateData(Exception):
    '''
    exceptions lost load documents
    '''
    def __init__(self, *args):
        super().__init__(*args)
    def __str__(self):
        return "Duplicate data in excel file"
