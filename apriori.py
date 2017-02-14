import pandas as pd
import itertools as IT
from datetime import date


def prepare_transactions(filepath=None, TID_cols=None, item_col=None,
                         stores=None, remove_non_sales=True):
    """
    TID_cols -> is a list of columns to concat and create unique TID
    item_col -> the column id that holds the items, or hierarchy to analyze
    """

    data = pd.read_excel(filepath)
    data['TID'] = ''

    non_sales_teams = ['Containers', 'Coupons', 'Customer Services', 'Promo',
                       'Unidentified Items']

    if stores is not None:
        data = data[data.loc[:, 'Store'].isin(stores)]

    if remove_non_sales is True:
        data = data[~data.loc[:, '01 Family'].isin(non_sales_teams)]

    for column_name in TID_cols:

        data['TID'] += data[column_name].astype(str)

    data['Item'] = data[item_col]

    data = data.loc[:, ['TID', 'Item']]

    return data


def apriori_f1(data, min_support=None):

    transaction_count = len(data['TID'].unique())

    data_grouped = data.groupby(['TID'])

    if min_support < 1:

        min_support = int(min_support*transaction_count)

        c_1 = {}

        for transaction in data_grouped.__iter__():
            for item in transaction[1]['Item']:

                if item not in c_1:

                    c_1[item] = 1

                else:

                    c_1[item] += 1

        f_1 = {item: support for item, support in c_1.items()
               if support >= min_support}

        return f_1


def unique_items(tuple_dict):

    unique_items = []

    for item_tuple in tuple_dict:

        if isinstance(item_tuple, basestring):
            if item_tuple not in unique_items:

                unique_items.append(item_tuple)

            else:
                for item in item_tuple:

                    if item not in unique_items:
                        unique_items.append(item)

    return unique_items


def new_generate_candidates(data, freq_itemset=None, k=None):

    if k > 1:
        freq_itemset = unique_items(freq_itemset.keys())
    else:
        freq_itemset = freq_itemset.keys()

    freq_itemset = list(IT.combinations(freq_itemset, k + 1))
    length = len(data.groupby(['TID']))
    candidates = {}

    transactions = [set(trx_items.unique()) for _, trx_items in
                    data.groupby(['TID'])['Item']]

    for num, itemset in enumerate(freq_itemset):

        print(str(num/len(freq_itemset)))

        subset = [subset for subset in transactions if set(itemset) <= subset]
        support = len(subset)

        if support > 0:
            itemset = tuple(sorted(itemset))
            candidates[itemset] = candidates.get(itemset, 0) + support

    return candidates


def membership_check(freq_itemset=None, previous_freq_itemset=None, k=None):

    items = {}

    for itemset, support in freq_itemset.items():
        unique_items = set()

        for item in IT.combinations(itemset, k):

            if len(item) == 1:
                item = item[0]
            else:
                item = tuple(sorted(item))

            unique_items.add(item)

        if unique_items <= set(previous_freq_itemset.keys()):
                items[itemset] = support

    return items


def prune_candidates(candidates, min_support):

    freq_itemset = {item: support for item, support in candidates.items()
                    if support >= min_support}

    return freq_itemset


def apriori(data, min_support=None):

    transaction_count = len(data['TID'].unique())

    if min_support < 1:
        min_support = int(min_support*transaction_count)

    freq_itemset = apriori_f1(data, min_support=min_support)

    k = 1
    loop = True
    while loop is True:

        candidates = new_generate_candidates(data,
                                             freq_itemset= freq_itemset,
                                             k = k)
        
        next_freq_itemset = prune_candidates(candidates, min_support=min_support)

        next_freq_itemset = membership_check(freq_itemset=next_freq_itemset,
                previous_freq_itemset=freq_itemset, k=k)
        
        k += 1          
        if len(candidates) == 0:
            break
        elif len(next_freq_itemset)==0:
            break
        else:
            freq_itemset = next_freq_itemset

    return freq_itemset


def association_rules(data, freq_itemset=None):
        
    rules = {}
    for itemset in freq_itemset.keys():
        for k in range(2, len(itemset)+1):
            for item in IT.permutations(itemset,k):
                
                item = list(item)
                y = item.pop()
                key = str(tuple(item)) + "->" + str(y)
                
                rules[key] = {
                                'Items': tuple(item),
                                'Target': y
                             }

        n = len(data['TID'].unique())

        arules = {}
        for num,transaction in enumerate(data.groupby(['TID']).__iter__()):
            print str(num) + " out of " + str(n) + "transactions"
            for key, values in rules.items():

                frq_x = 0
                frq_y = 0
                frq_xy = 0
                
                trx_items = set(transaction[1]['Item'].unique())

                x = set(list(values['Items']))
                y = set([values['Target']])
                xy = set(list(values['Items']) + [values['Target']])
                
                if x <= trx_items:
                    frq_x += 1
                
                if y <= trx_items:
                    frq_y += 1
                
                if xy <= trx_items:
                    frq_xy += 1

                if key not in arules:
                    arules[key] = {
                                    'Items' : values['Items'],
                                    'Target' : values['Target'],
                                    'Frequency X' : frq_x,
                                    'Frequency Y' : frq_y,
                                    'Frequency XY' : frq_xy
                                  }

                else:
                    arules[key]['Frequency X'] += frq_x
                    arules[key]['Frequency Y'] += frq_y
                    arules[key]['Frequency XY'] += frq_xy

    return arules, n


def calculate_arules(arules, n):

    arulesDF = pd.DataFrame(arules).T

    arulesDF['Support'] = arulesDF['Frequency XY'] / n
    arulesDF['Confidence'] = arulesDF['Frequency XY'] / arulesDF['Frequency X']
    arulesDF['Lift'] = arulesDF['Support'] / ((arulesDF['Frequency X']/n) * (arulesDF['Frequency Y']/n))

    arules = arulesDF.loc[: , ['Items', 'Target', 'Support', 'Confidence', 'Lift']]

    return arules


if __name__ == '__main__':
        
    data = prepare_transactions(filepath="TRX_2016_12_28-2017_01_03.xlsx", 
        TID_cols=['Date', 'Unnamed: 2', 'Unnamed: 4', 'Transaction Time', 'Store'], 
        item_col='02 Category',
        # stores=['Silver Lake'],#, 'Lake Oswego', 'Bellevue Square'],
        stores=['Bellevue Square'],
        remove_non_sales=True)

#    data = prepare_transactions(filepath="TRX_Apriori_Test.xlsx", 
#        TID_cols=['Transaction'], 
#        item_col='item',
#        stores=None,
#        remove_non_sales=False)

    items = apriori(data, min_support=2)

    arules, n = association_rules(data, freq_itemset=items)

    arulesDF = calculate_arules(arules, n)

    arulesDF.to_excel('Arules.xlsx')
