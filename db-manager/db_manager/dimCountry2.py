from . import utils

def stage_csv(state, file_path):
    return {'country': utils.read_csv(file_path)}

def applyChanges(state, source=None):
    print('got state', state)
    pass
