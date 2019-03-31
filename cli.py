from __future__ import print_function, unicode_literals
from PyInquirer import style_from_dict, Token, prompt, Separator
from music_library_db import *

import csv, terminaltables, functools


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
operations = ["INSERT", "UPDATE", "DELETE", "SELECT", "QUIT"]
operation_choices = [{'name': operation} for operation in operations]


def make_question(type, message, name, choices):
    return {'type': type,
            'message': message,
            'name': name,
            'choices': choices}


def prompt_list(name, what_message, choices_names):
    answer = prompt([make_question('list', 'Select '+what_message, name,
                                   [{'name': choice_name} for choice_name in choices_names])])
    return answer[name]


def prompt_entities():
    return prompt_list('entity', 'an entity', [entity['name'] for entity in entities])


def get_entity_by_name(entity_name):
    return next(entity for entity in entities if entity['name'] == entity_name)


def get_entity_attributes(entity_name):
    return get_entity_by_name(entity_name)['attributes']


def get_names(attributes_data):
    return [attribute['name'] for attribute in attributes_data]


def get_entity_attributes_names(entity_name):
    return get_names(get_entity_attributes(entity_name))


def get_entity_text_attributes(entity_name):
    return list(filter(lambda attr_data: attr_data['type'] == 'VARCHAR',
                       get_entity_attributes(entity_name)))


def get_entity_text_attributes_names(entity_name):
    return get_names(get_entity_text_attributes(entity_name))


def prompt_attributes(entity_name):
    return prompt_list('attribute', 'an attribute', get_entity_attributes_names(entity_name))


def prompt_text_attributes(entity_name):
    return prompt_list('attribute', 'among text attributes', get_entity_text_attributes_names(entity_name))


csv_file = 'artists.csv'
json_file = 'artist.json'


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
        'Album_Id': identity,
        'Year': identity
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
        'Album_Id': validate_integer,
        'Year': validate_integer
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


def make_input_for_attribute(entity_name, attribute_name):
    return {
        'type': 'input',
        'name': attribute_name,
        'message': 'Enter the ' + attribute_name,
        'validate': attribute_type_validators[entity_name][attribute_name],
        'filter': attribute_type_filters[entity_name][attribute_name]
    }


def validate_single_word(string):
    return len(string.split()) == 1


def make_input_for_attribute_single_word_key(entity_name, attribute_name):
    return {
        'type': 'input',
        'name': attribute_name,
        'message': 'Enter the ' + attribute_name,
        'validate': attribute_type_validators[entity_name][attribute_name],
        'filter': attribute_type_filters[entity_name][attribute_name]
    }


def make_inputs_for_attributes(entity_name, attributes):
    inputs = list(map(lambda attribute: make_input_for_attribute(entity_name, attribute['name']),
                      attributes))
    return inputs


def print_table(entity_name, rows):
    attribute_name = [tuple(i for i in get_entity_attributes_names(entity_name))]
    table_wrapper = terminaltables.SingleTable(attribute_name + rows)
    print(table_wrapper.table)


def print_join(entity1_name, entity2_name, rows):
    attribute1_name = [i for i in get_entity_attributes_names(entity1_name)]
    attribute2_name = [i for i in get_entity_attributes_names(entity2_name)]
    attribute_name = [tuple(attribute1_name + attribute2_name)]
    table_wrapper = terminaltables.SingleTable(attribute_name + rows)
    print(table_wrapper.table)


def print_entity(db, entity_name):
    rows = db.select_all(entity_name)
    print_table(entity_name, rows)


def print_fetch_all(db):
    print(db.fetch_all())


def prompt_input_attributes(entity_name):
    entity_data = get_entity_by_name(entity_name)
    return prompt(make_inputs_for_attributes(entity_name, entity_data['attributes']))


def perform_insert(db):
    entity_name = prompt_entities()
    print_entity(db, entity_name)
    attributes_row = prompt_input_attributes(entity_name)
    db.insert(into=entity_name, row=attributes_row)
    print_entity(db, entity_name)


def perform_delete(db):
    entity_name = prompt_entities()
    db.delete_all(entity_name)


def perform_update(db):
    entity_name = prompt_entities()
    print(entity_name)


def make_fulltext_handler():
    def get_search_data(db, prompter, searcher):
        entity_name = prompt_entities()
        print(entity_name)
        print_entity(db, entity_name)
        attribute_name = prompt_text_attributes(entity_name)
        key_questions = [prompter(entity_name, attribute_name)]
        key = prompt(key_questions)[attribute_name]

        search_args = {
            'entity': entity_name,
            'attribute': attribute_name,
            'key': key
        }

        rows = searcher(db, search_args)
        print_table(entity_name, rows)

    def all_occur(db):
        get_search_data(
            db, make_input_for_attribute_single_word_key,
            lambda db, search_args: db.fulltext_search_all_match(**search_args)
        )

    def one_doesnt(db):
        get_search_data(
            db, make_input_for_attribute,
            lambda db, search_args: db.fulltext_search_one_not(**search_args)
        )

    modes = ("all words occur", "one word doesn't occur")
    modes_handlers = {
        "all words occur": all_occur,
        "one word doesn't occur": one_doesnt
    }

    def do(db):
        mode = prompt_list('modes', 'the search mode', modes)
        handler = modes_handlers[mode]
        handler(db)

    return do


fulltext_handler = make_fulltext_handler()


def make_select_join_handler():
    joins = [
        ('Artist', 'release'),
        ('release', 'Album'),
        ('Track', 'Album')
    ]

    def do(db):
        entity1_name = prompt_list('entity', 'the first entity', [entity['name'] for entity in entities])
        joinable_entities = iter(join for join in joins if entity1_name in join)
        joinable_entities_flatten = functools.reduce(lambda t, s: list(t)+list(s),
                                                     joinable_entities)
        entities_without_selected = [e for e in joinable_entities_flatten if e != entity1_name]
        #entities_names = [entity['name'] for entity in entities_without_selected]
        entity2_name = prompt_list('entity', 'the second entity', entities_without_selected)
        rows = db.select_inner_join(entity1_name, entity2_name)
        print_join(entity1_name, entity2_name, rows)

    return do


select_join_handler = make_select_join_handler()


def perform_select_mixin():
    options = ["fulltext search", "join"]
    handlers = {"fulltext search": fulltext_handler,
                "join": select_join_handler}

    def do(db):
        option = prompt_list('option', 'the SELECT option', options)
        handler = handlers[option]
        return handler(db)

    return do


perform_select = perform_select_mixin()

operations_performers = {
    "INSERT": perform_insert,
    "DELETE": perform_delete,
    "UPDATE": perform_update,
    "SELECT": perform_select
}


def prompt_main_menu(db):
    operation_name = prompt_list('operation', 'the operation to perform', operations)
    if operation_name == "QUIT":
        return operation_name
    performer = operations_performers[operation_name]
    return performer(db)


def main(db):
    try:
        while prompt_main_menu(db) != "QUIT":
            pass
    except psycopg2.IntegrityError as error:
        print(error.pgerror)


if __name__ == '__main__':
    connection_parameters = {
        'user': 'postgres', 'host': 'localhost', 'password': 'py', 'database': 'db1'
    }
    with MusicLibraryDatabase([], **connection_parameters) as db_handle:
        main(db_handle)

# entity_name = prompt_list('entity', 'an entity', [entity['name'] for entity in entities])

"""
{'type': 'list',
     'message': 'Enter the attributes',
     'name': 'entity',
     'choices': entities_choices,
     'validate': lambda answer: print(answer) or 'Sorry, try again' if len(answer) == 0 else True,
     'when': lambda answer: (print(answer) and False) or answer['operation'] == "INSERT"}
     """

