import shelve
import fnmatch
import re
from contextlib import contextmanager

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

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

    # Full method names
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

    # Compact method names (mapped to full versions)
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

    def __init__(self, file_name: str):
        ''' Initialize the ShelveDB instance with the given file name.

        Args:
            file_name (str): The file name of the shelve database.
        '''
        self.file_name = file_name

    def __enter__(self) -> "ShelveDB":
        ''' Open the shelve database and return the instance when entering a context.

        Returns:
            ShelveDB: The current instance of the ShelveDB class.
        '''
        self.db = shelve.open(self.file_name)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        ''' Close the shelve database when exiting the context.
        '''
        self.db.close()

    @contextmanager
    def open_db(self) -> shelve.DbfilenameShelf:
        ''' A context manager to open and close the shelve database.

        Yields:
            shelve.DbfilenameShelf: An opened shelve database.
        '''
        # Open the shelve database with the given file name
        db = shelve.open(self.file_name)

        try:
            # Yield the opened shelve database to the caller
            yield db
        finally:
            # Close the shelve database when exiting the context,
            # ensuring proper resource management even in case of exceptions
            db.close()

    def new(self, value: Dict[str, Any]) -> str:
        ''' Create a new entry in the shelve database with a unique key.

        Args:
            value (Dict[str, Any]): A dictionary representing the value to be inserted.

        Returns:
            str: The newly generated unique key for the inserted value.
        '''
        # Use the context manager to open the shelve database
        with self.open_db() as db:
            # Get a list of integer keys from the database
            keys = [int(key) for key in db.keys() if key.isdigit()]

            # Calculate the new key as the maximum existing key + 1, or use "1" if there are no existing keys
            new_key = str(max(keys) + 1) if keys else "1"

            # Insert the new value into the database with the new key
            db[new_key] = value

            # Return the new key
            return new_key

    def insert(self, key: str, value: Dict[str, Any]):
        ''' Insert a new key-value pair into the shelve database.

        Args:
            key (str): The key to use for the new entry.
            value (Dict[str, Any]): The value to associate with the key.
        '''
        # Use the context manager to open and close the shelve database
        with self:
            # Store the value with the given key in the database
            self.db[key] = value

    def update(self, key: str, new_values: Dict[str, Any]):
        ''' Update the values of an existing key in the shelve database.

        Args:
            key (str): The key of the entry to update.
            new_values (Dict[str, Any]): The new values to update the entry with.
        '''
        # Use the context manager to open and close the shelve database
        with self:
            # Check if the key exists in the database
            if key in self.db:
                # Get the current values of the key
                current_values = self.db[key]
                # Update the current values with the new values
                current_values.update(new_values)
                # Store the updated values back in the database
                self.db[key] = current_values

    def delete(self, key: str):
        ''' Delete an existing key from the shelve database.

        Args:
            key (str): The key of the entry to delete.
        '''
        # Use the context manager to open and close the shelve database
        with self:
            # Check if the key exists in the database
            if key in self.db:
                # Delete the key from the database
                del self.db[key]

    def clear(self):
        ''' Clear all items from the shelve database.
        '''
        # Use the context manager to open and close the shelve database
        with self:
            # Clear all items from the database
            self.db.clear()

    def query(self, conditions: Optional[List[Callable[[Tuple[str, Dict[str, Any]]], bool]]] = None, select_columns: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        ''' Query the shelve database with the given conditions and return the selected columns.

        Args:
            conditions (List[function], optional): a list of lambda functions that filter the items in the database.
            select_columns (List[str], optional): a list of column names to select. If None, select all columns.

        Returns:
            Dict[str, Dict[Any, Any]]: a dictionary where the keys are the selected keys and the values are dictionaries of the selected columns.
        '''
        # If no conditions are provided, use an empty list
        if conditions is None:
            conditions = []

        # Use the context manager to open and close the shelve database
        with self:
            # Get all items from the database
            items = self.db.items()

            # Apply each condition by filtering the items using the condition
            for condition in conditions:
                items = filter(condition, items)

            # If no select_columns are provided, return all columns for the filtered items
            if select_columns is None:
                return dict(items)

            # If select_columns are provided, return only the specified columns for the filtered items
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

        # Update '2' values
        db.update('2', {'age': 31, 'city': 'San Francisco'})

        # Delete '3'
        db.delete('3')

        # Create a new item with a unique key
        new_item_key = db.new({'name': 'Tom', 'age': 35, 'city': 'Seattle'})
        print(f'New item created with key: {new_item_key}')
        # RESULT: 
        # New item created with key: 3

        # Clear all data (uncomment if you want to clear the database)
        # db.clear()

        # Query users with age > 30, selecting only 'name' and 'city' columns
        result = db.query(
            conditions=[QueryConditions.gt('age', 30)],
            select_columns=['name', 'city']
        )

    print(result)
    # RESULT: 
    # {'2': {'name': 'Alice', 'city': 'San Francisco'}, '3': {'name': 'Tom', 'city': 'Seattle'}}
