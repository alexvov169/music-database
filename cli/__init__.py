RADIOBUTTON = {'button-type': 'radiobutton'}
CHECKBOX = {'button-type': 'checkbox'}
INSERT_MENU = {'menu': {}}
EDIT_MENU = {'menu': {}}
DELETE_MENU = {'menu': {}}
ENTITIES_OPTIONS = {}
SELECT_MENU = {'menu': {'prompt': "Select entities",
                        'options': ENTITIES_OPTIONS,
                        'settings': CHECKBOX}}
MENU = {'menu': {'prompt': 'What do you want?',
                 'options': [{'option': {'text': "insert",
                                         'do': INSERT_MENU}},
                             {'option': {'text': "edit",
                                         'do': EDIT_MENU}},
                             {'option': {'text': "delete",
                                         'do': DELETE_MENU}},
                             {'option': {'text': "select",
                                         'do': SELECT_MENU}}],
                 'settings': RADIOBUTTON}}