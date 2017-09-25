import threading

from blessings import Terminal

from cook.util import print_info

term = Terminal()
data = []
lock = threading.Lock()


def print_state(lines_to_erase):
    """
    "Refreshes" the state on the terminal by moving the cursor up
    lines_to_erase lines and then printing the current state of the data
    list, which contains [item, status] pairs.
    """
    with term.location(0, term.height - lines_to_erase - 1):
        print_info('\n'.join([('%s ... %s' % (i, s)) for [i, s] in data]))


def add(item):
    """
    Adds a new item (with blank status) and prints the new state
    """
    with lock:
        index = len(data)
        data.append([item, ''])
        print_state(index)
        return index


def update(index, status):
    """
    Updates the status of the item with the given index and prints the new state
    """
    with lock:
        data[index][1] = status
        print_state(len(data))
