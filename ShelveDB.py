import shelve, fnmatch, re

import shelve
import fnmatch
import re
from typing import Any, Callable, Dict, List, Tuple, Union

class QueryConditions:
    ''' A class that provides query conditions for the ShelveDB class.'''

    @staticmethod
    def _generate_lambda(column: str, value: Any, comparison_fn: Callable[[Any, Any], bool]) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        '''
        Generates a lambda function that filters items based on the comparison function.

        Args:
            column (str): Column name to compare.
            value (Any): Value to compare with.
            comparison_fn (Callable[[Any, Any], bool]): Comparison function to use.

        Returns:
            Callable[[Tuple[str, Dict[str, Any]]], bool]: Lambda function for filtering.
        '''
        return lambda item: item[1].get(column) and comparison_fn(item[1][column], value)

    # Full method names (mapped to compact versions)
    @staticmethod
    def greater_than(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column is greater than the given value.
        '''
        return QueryConditions._generate_lambda(column, value, lambda x, y: x > y)

    @staticmethod
    def equals(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column is equal to the given value.
        '''
        return QueryConditions._generate_lambda(column, value, lambda x, y: x == y)

    @staticmethod
    def not_equals(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column is not equal to the given value.
        '''
        return QueryConditions._generate_lambda(column, value, lambda x, y: x != y)

    @staticmethod
    def contains(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column contains the given value.
        '''
        return QueryConditions._generate_lambda(column, value, lambda x, y: y in x)

    @staticmethod
    def not_contains(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column does not contain the given value.
        '''
        return QueryConditions._generate_lambda(column, value, lambda x, y: y not in x)

    @staticmethod
    def wildcard(column: str, pattern: str) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column matches the given wildcard pattern.
        '''
        return QueryConditions._generate_lambda(column, pattern, lambda x, y: fnmatch.fnmatch(x, y))

    @staticmethod
    def regex(column: str, pattern: str) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Return a lambda function that filters items where the value of the column matches the given regex pattern.
        '''
        return QueryConditions._generate_lambda(column, pattern, lambda x, y: re.search(y, x))

    # Compact method names
    @staticmethod
    def gt(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for greater_than.
        '''
        return QueryConditions.greater_than(column, value)
        
    @staticmethod
    def eq(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for equals.
        '''
        return QueryConditions.equals(column, value)

    @staticmethod
    def ne(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for not_equals.
        '''
        return QueryConditions.not_equals(column, value)

    @staticmethod
    def ct(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for contains.
        '''
        return QueryConditions.contains(column, value)

    @staticmethod
    def nct(column: str, value: Any) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for not_contains.
        '''
        return QueryConditions.not_contains(column, value)

    @staticmethod
    def wc(column: str, pattern: str) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for wildcard.
        '''
        return QueryConditions.wildcard(column, pattern)

    @staticmethod
    def re(column: str, pattern: str) -> Callable[[Tuple[str, Dict[str, Any]]], bool]:
        ''' Alias for regex.
        '''
        return QueryConditions.regex(column, pattern)
    
class ShelveDB:
    ''' A class that provides CRUD operations and querying on a shelve database.
    '''

    def __init__(self, file_name):
        self.file_name = file_name

    def __enter__(self):
        self.db = shelve.open(self.file_name)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def new(self, value):
        with shelve.open(self.file_name) as db:
            keys = list(db.keys())
            if keys:
                last_key = max([int(key) for key in keys if key.isdigit()])
                new_key = str(last_key + 1)
            else:
                new_key = "1"
            db[new_key] = value
            return new_key
        
    def insert(self, key, value):
        ''' Insert a new key-value pair into the shelve database.
        '''
        with self:
            self.db[key] = value

    def update(self, key, new_values):
        ''' Update the values of an existing key in the shelve database.
        '''
        with self:
            if key in self.db:
                current_values = self.db[key]
                current_values.update(new_values)
                self.db[key] = current_values

    def delete(self, key):
        ''' Delete an existing key from the shelve database.
        '''
        with self:
            if key in self.db:
                del self.db[key]

    def clear(self):
        ''' Clear all items from the shelve database.
        '''
        with self:
            self.db.clear()

    def query(self, conditions=None, select_columns=None):
        ''' Query the shelve database with the given conditions and return the selected columns.

        Args:
            conditions (List[function]): a list of lambda functions that filter the items in the database.
            select_columns (List[str]): a list of column names to select. If None, select all columns.

        Returns:
            Dict[str, Dict[Any, Any]]: a dictionary where the keys are the selected keys and the values are dictionaries of the selected columns.
        '''
        if conditions is None:
            conditions = []

        with self:
            items = self.db.items()

            for condition in conditions:
                items = filter(condition, items)

            if select_columns is None:
                return dict(items)

            return {
                key: {column: value[column] for column in select_columns if column in value}
                for key, value in items
            }

if __name__ == '__main__':
    
    # Example usage
    with ShelveDB('my_shelve.db') as db:

        # Insert example data
        db.insert('1', {'name': 'John', 'age': 25, 'city': 'New York'})
        db.insert('2', {'name': 'Alice', 'age': 30, 'city': 'Los Angeles'})
        db.insert('3', {'name': 'Bob', 'age': 40, 'city': 'Chicago'})

        # Update '4' values
        db.update('2', {'age': 31, 'city': 'San Francisco'})

        # Delete '3'
        db.delete('3')

        # Create a new item with a unique key
        new_item_key = db.new({'name': 'Tom', 'age': 35, 'city': 'Seattle'})
        print(f'New item created with key: {new_item_key}')

        # Clear all data
        # db.clear()

        # Query users with age > 30, selecting only 'name' and 'city' columns
        result = db.query(
            conditions=[QueryConditions.gt('age', 30)],
            select_columns=['name', 'city']
        )

    print(result)
