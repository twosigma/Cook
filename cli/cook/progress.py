import threading

from cook import terminal
from cook.util import print_info

data = []
lock = threading.Lock()


def __print_state(lines_to_move_up):
    """
    "Refreshes" the state on the terminal by moving the cursor up
    lines_to_move_up lines and then printing the current state of the data
    list, which contains [item, status] pairs.
    """
    print_info(terminal.MOVE_UP * lines_to_move_up, end='')
    print_info('\n'.join([f'{item} ... {state}' for [item, state] in data]))


def add(item):
    """
    Adds a new item (with blank status) and prints the new state.
    """
    with lock:
        index = len(data)
        data.append([item, ''])
        __print_state(index)
        return index


def update(index, status):
    """
    Updates the status of the item with the given index and prints the new state.
    """
    with lock:
        data[index][1] = status
        __print_state(len(data))
