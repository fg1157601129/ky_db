import os


def get_base_path():
    """
    :return:
    """
    path = os.path.dirname(__file__)
    return path


if __name__ == '__main__':
    root_path = get_base_path()
    print(root_path)