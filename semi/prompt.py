

def is_question_answer_yes(question: str) -> bool:
    """
    Prompt a yes/no question to the user.

    Parameters
    ----------
    question : str
        The question to print on stdout.

    Returns
    bool
        True if the user answers "yes", False otherwise.
    """

    answer = input(question + " [y/N] ")
    answer = answer.lower()
    if answer == "y" or answer == "yes":
        return True
    return False


def let_user_pick(options: list) -> int:
    """
    Present a list of options to the user for selection.

    Parameters
    ----------
    options : list
        A list of options to be printed and user to choose from.

    Returns
    -------
    int
        The index of the users selection.
    """

    print("Please choose:")
    for idx, element in enumerate(options):
        print(f"{idx + 1}) {element}")
    choice = input("Enter number: ")
    try:
        if 0 < int(choice) <= len(options):
            return int(choice) - 1
    except:
        pass
    print("Not a valid choice choose again!")
    return let_user_pick(options)