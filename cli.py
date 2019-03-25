from __future__ import print_function, unicode_literals
from PyInquirer import style_from_dict, Token, prompt, Separator
from music_library_db import *

import csv, terminaltables


def make_style(color):
    return style_from_dict({
        Token.Separator: color,
        Token.QuestionMark: '#673ab7 bold',
        Token.Selected: color,  # default
        Token.Pointer: '#673ab7 bold',
        Token.Instruction: '',  # default
        Token.Answer: '#f44336 bold',
        Token.Question: '',
    })


green_style = make_style('#008000')
red_style = make_style('#cc5454')
entities = DATABASE_DATA['entities']
operations = ["INSERT", "UPDATE", "DELETE", "SELECT"]
operation_choices = [{'name': operation} for operation in operations]
entities_choices = [{'name': entity['name']} for entity in entities]
qq = [
    {'type': 'list',
     'message': 'Select the operation to perform',
     'name': 'operation',
     'choices': operation_choices},
    {'type': 'list',
     'message': 'Select an entity',
     'when': lambda answer: answer['operation'] in operations,
     'name': 'entity',
     'choices': entities_choices},
]


def make_question(type, message, name, choices):
    return {'type': type,
            'message': message,
            'name': name,
            'choices': choices}


def get_entity_by_name(entity_name):
    return next(entity for entity in entities if entity['name'] == entity_name)


def get_entity_attributes(entity_name):
    return get_entity_by_name(entity_name)['attributes']


def get_entity_attributes_names(entity_name):
    return [attribute['name'] for attribute in get_entity_attributes(entity_name)]


def prompt_attributes(entity_name):
    attribute_choices = [{'name': attribute} for attribute in get_entity_attributes_names(entity_name)]
    questions = [make_question('list', 'Select an attribute', 'attribute', attribute_choices)]
    return prompt(questions, style=red_style)


csv_file = 'artists.csv'
json_file = 'artist.json'
connection_parameters = {
    'user': 'postgres', 'host': 'localhost', 'password': 'py', 'database': 'db1'
}


def capitalize_all_words(val):
    return ' '.join([word.capitalize() for word in val.split()])


def validate_varchar(value): return len(value.split()) != 0


def validate_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def identity(x): return x


attribute_type_filters = {
    'Artist': {
        'Artist_Name': capitalize_all_words,
        'Artist_Id': identity
    },
    'Album': {
        'Album_Name': capitalize_all_words,
        'Album_Id': identity
    },
    'Playlist': {
        'Filepath': identity,
        'Description': identity,
        'Playlist_Name': identity
    },
    'Track': {
        'Track_Name': capitalize_all_words,
        'Track_Id': identity,
        'Number': identity
    },
    'release': {
        'Track_Id': identity,
        'Artist_Id': identity
    }
}
attribute_type_validators = {
    'Artist': {
        'Artist_Name': validate_varchar,
        'Artist_Id': validate_integer
    },
    'Album': {
        'Album_Name': validate_varchar,
        'Album_Id': validate_integer
    },
    'Playlist': {
        'Filepath': validate_varchar,
        'Description': validate_varchar,
        'Playlist_Name': validate_varchar
    },
    'Track': {
        'Track_Name': validate_varchar,
        'Track_Id': validate_integer,
        'Number': validate_integer
    },
    'release': {
        'Track_Id': validate_integer,
        'Artist_Id': validate_integer
    }
}


def make_input_for_attribute(entity_name, attribute):
    attribute_name = attribute['name']
    # attribute_type = attribute['type']
    # print(attribute_type)
    return {
        'type': 'input',
        'name': attribute_name,
        'message': 'Enter ' + attribute_name,
        'validate': attribute_type_validators[entity_name][attribute_name],
        'filter': attribute_type_filters[entity_name][attribute_name]
    }


def make_inputs_for_attributes(entity_name, attributes):
    inputs = list(map(lambda attribute: make_input_for_attribute(entity_name, attribute),
                      attributes))
    # print(inputs)
    return inputs


# def perform_operation(**operation_data):
# if True:
# print(operation_data)
# print(db.select_all('Artist'))

# data = db.select_all('Artist')
# data = db.fulltext_search_all_match(entity='Artist', attribute='Artist_Name', key='Nirvana')
# table_wrapper = terminaltables.SingleTable([('Artist', 'Id')] + data)
# print(table_wrapper.table)


def print_table(db, entity_name):
    rows = db.select_all(entity_name)
    attribute_name = [tuple(i for i in get_entity_attributes_names(entity_name))]
    table_wrapper = terminaltables.SingleTable(attribute_name + rows)
    print(table_wrapper.table)


def perform_insert(db, entity_name):
    print_table(db, entity_name)
    entity_data = get_entity_by_name(entity_name)
    attributes_row = prompt(make_inputs_for_attributes(entity_name, entity_data['attributes']))
    db.insert(into=entity_name, row=attributes_row)
    print_table(db, entity_name)


def perform_delete(db, entity_name):
    db.delete_all(entity_name)


def perform_update(db, entity_name):
    pass


def perform_select(db, entity_name):
    pass


operations_performers = {
    "INSERT": perform_insert,
    "DELETE": perform_delete,
    "UPDATE": perform_update,
    "SELECT": perform_select
}
joins = {
    'Artist': 'Artist_Id',
    'Album': 'Album_Id',
    'Playlist': 'Filepath',
    'Track': 'Track_Id',
    'release': {
        'Track_Id',
        'Artist_Id'
    }
}


def main(db):
    operation_and_entity_or_quit = prompt(qq, style=green_style)
    entity_name = operation_and_entity_or_quit['entity']
    operation = operation_and_entity_or_quit['operation']
    try:
        performer = operations_performers[operation]
    except ValueError:
        operation_and_entity = operation_and_entity_or_quit
        attribute = prompt_attributes(operation_and_entity['entity'])
    else:
        performer(db, entity_name)


if __name__ == '__main__':
    with MusicLibraryDatabase([], **connection_parameters) as db_handle:
        main(db_handle)

"""
{'type': 'list',
     'message': 'Enter the attributes',
     'name': 'entity',
     'choices': entities_choices,
     'validate': lambda answer: print(answer) or 'Sorry, try again' if len(answer) == 0 else True,
     'when': lambda answer: (print(answer) and False) or answer['operation'] == "INSERT"}
     """
