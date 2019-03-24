from __future__ import print_function, unicode_literals
from PyInquirer import style_from_dict, Token, prompt, Separator
from music_library_db import *

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

style = style_from_dict({
    Token.Separator: '#cc5454',
    Token.QuestionMark: '#673ab7 bold',
    Token.Selected: '#008000',  # default
    Token.Pointer: '#673ab7 bold',
    Token.Instruction: '',  # default
    Token.Answer: '#f44336 bold',
    Token.Question: '',
})


entities_choices = [{'name': entity['name']} for entity in DATABASE_DATA['entities']]
qq = [
    {'type': 'list',
     'message': 'Select the operation to perform',
     'name': 'operation',
     'choices': [{'name': "INSERT"},
                 {'name': "EDIT"},
                 {'name': "DELETE"},
                 {'name': "SELECT"},
                 {'name': "QUIT"}]},
    {'type': 'list',
     'message': 'Select the operation to perform',
     'name': 'entity',
     'choices': entities_choices,
     'validate': lambda answer: print(answer) or 'Sorry, try again' if len(answer) == 0 else True,
     'when': lambda answer: (print(answer) and False) or answer['operation'] == "INSERT"}
]


def main():
    prompt(qq, style=style)


if __name__ == '__main__':
    main()
