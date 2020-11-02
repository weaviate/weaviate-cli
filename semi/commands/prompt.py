
def prompt_yes_no(question):
    """ Prompt a yes/no question to the user
    :param question:
    :return: true if the user answers yes
    :rtype: bool
    """
    answer = input(question)
    answer = answer.lower()
    if answer == "y" or answer == "yes":
        return True
    return False