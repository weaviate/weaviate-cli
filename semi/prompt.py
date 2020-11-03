
def is_question_answer_yes(question):
    """ Prompt a yes/no question to the user
    :param question:
    :return: true if the user answers yes
    :rtype: bool
    """
    answer = input(question+" [y/N] ")
    answer = answer.lower()
    if answer == "y" or answer == "yes":
        return True
    return False


def let_user_pick(options: list):
    """ Present a list of options to the user for selection.

    :param options:
    :return: the index of the users selection
    :rtype int
    """
    print("Please choose:")
    for idx, element in enumerate(options):
        print("{}) {}".format(idx+1,element))
    i = input("Enter number: ")
    try:
        if 0 < int(i) <= len(options):
            return int(i)-1
    except:
        pass
    print("Not a valid choice choose again!")
    return let_user_pick(options)