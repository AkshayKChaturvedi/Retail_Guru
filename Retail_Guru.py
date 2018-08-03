import pandas as pd
from fim import *


def read_and_preprocess_data(path):
    articles_data = pd.read_csv(path + '/articles.csv')
    article_hierarchy_data = pd.read_csv(path + '/article_hierarchy.csv')
    bills_data = pd.read_csv(path + '/bills.csv')

    bills_data = bills_data.drop_duplicates()
    article_hierarchy_data = article_hierarchy_data.drop_duplicates()
    articles_data = articles_data.drop_duplicates()
    articles_data = articles_data.rename(columns={'name': 'article_name'})

    return articles_data, article_hierarchy_data, bills_data


def merge_and_prepare_final_data(articles_data, article_hierarchy_data, bills_data, return_type=None):
    bills_articles = bills_data.merge(articles_data, on=['article_id', 'store_id'])

    bills_articles_with_hierarchy = bills_articles.merge(article_hierarchy_data, on=['article_id', 'store_id'])

    columns_to_keep = ['store_id', 'sale_date', 'article_id', 'customer_code', 'article_name', 'category_name',
                       'subcategory_name']

    bills_articles_with_hierarchy = bills_articles_with_hierarchy.drop(
        axis=1, labels=list(set(bills_articles_with_hierarchy.columns) - set(columns_to_keep)))

    bills_articles_with_hierarchy['sale_date'] = pd.to_datetime(bills_articles_with_hierarchy['sale_date'])

    if return_type == 'monthly_basket':
        monthly_basket_article = list(bills_articles_with_hierarchy.query('sale_date.dt.day <= 7').groupby(
            'customer_code')['article_name'].apply(list))

        return monthly_basket_article

    items_bought_on_a_day = list(bills_articles_with_hierarchy.groupby(
        ['sale_date', 'customer_code'])['article_name'].apply(list))
    
    return items_bought_on_a_day


def find_combo_items(list_items_bought_on_a_day, supp=2, report='S', zmin=2):

    frequent_item_sets = fpgrowth(list_items_bought_on_a_day, supp=supp, report=report, zmin=zmin)
    
    return frequent_item_sets


def display_results(return_type=None, path=None, supp=2, report='S', zmin=2):
    articles, article_hierarchy, bills = read_and_preprocess_data(path)

    items_list = merge_and_prepare_final_data(articles, article_hierarchy, bills, return_type)

    combo_items = find_combo_items(items_list, supp, report, zmin)

    df_combo_items = pd.DataFrame(data=combo_items, columns=['Items', 'Support%'])

    return df_combo_items


# Result for Task B
path = 'C:/Users/Dell/Desktop/Delium'
combo_items_list = display_results(path=path, supp=1.5)
print('combo_items:\n', combo_items_list)
combo_items_list.to_csv(path + '/combo_items.csv')

# Result for Task A
combo_items_list_2 = display_results(path=path, supp=5, return_type='monthly_basket')
print('monthly_basket:\n', combo_items_list_2)
combo_items_list_2.to_csv(path + '/monthly_basket.csv')
