## run all countries 

import subprocess

commands = [
    "python twitter_retrieve_by_user_through_search.py -name France --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name Germany --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name Italy --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name Japan --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name Mexico --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name Norway --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name UK --user_info --tweet",
    "python twitter_retrieve_by_user_through_search.py -name US --user_info --tweet"
]

for cmd in commands:
    result = subprocess.run(cmd, shell=True, capture_output=False, text=True)
    if result.returncode == 0:
        print("Success: {}".format(cmd))
    else:
        print("Error: {}".format(cmd))