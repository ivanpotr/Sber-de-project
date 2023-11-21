# Загрузка библиотек
import os


def current_date(path_load):
    list_transactions = []
    list_terminals = []
    list_passport_blacklist = []

    for root, dirs, files in os.walk(path_load):
        if root == path_load:
            for filename in files:
                if filename.startswith('transactions_'):
                    file_name_without_extension = os.path.splitext(filename)[0]
                    date_string = file_name_without_extension.split('_')[-1]
                    list_transactions.append(date_string)
                elif filename.startswith('terminals_'):
                    file_name_without_extension = os.path.splitext(filename)[0]
                    date_string = file_name_without_extension.split('_')[-1]
                    list_terminals.append(date_string)
                elif filename.startswith('passport_blacklist_'):
                    file_name_without_extension = os.path.splitext(filename)[0]
                    date_string = file_name_without_extension.split('_')[-1]
                    list_passport_blacklist.append(date_string)

    max_len_lest = max(len(list_transactions), len(
        list_terminals), len(list_passport_blacklist))

    index = 0
    cur_date_list = []
    while index < max_len_lest:
        if len(list_transactions) != 0:
            if list_transactions[index] in list_terminals and list_transactions[index] in list_passport_blacklist:
                cur_date_list.append(list_transactions[index])
            elif list_transactions[index] in list_terminals or list_transactions[index] in list_passport_blacklist:
                cur_date_list.append(list_transactions[index])
            else:
                cur_date_list.append(list_transactions[index])
        elif len(list_terminals) != 0:
            if list_terminals[index] in list_passport_blacklist:
                cur_date_list.append(list_terminals[index])
            else:
                cur_date_list.append(list_terminals[index])
        else:
            cur_date_list.append(list_passport_blacklist[index])
        index += 1

    cur_date_list.sort()
    return cur_date_list
