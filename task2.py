## task2 working final

from pyspark import SparkContext
import itertools
from itertools import combinations
import collections
import time
from functools import reduce
from operator import add
import sys


def main(argv):
    fil_threshold = int(sys.argv[1])
    supp = int(sys.argv[2])
    inputfile = sys.argv[3]
    outputfile = sys.argv[4]

    # case_num = 1
    # supp = 4
    # inputfile = 'small1.csv'
    # outputfile = 'out1_4.txt'

    path_new = 'customer_product.csv'

    def preprocess_tafeng(inputfile, path_new):

        sc = SparkContext.getOrCreate()


        lines = sc.textFile(inputfile)

        lines_header = lines.first()
        lines = lines.filter(lambda row: row != lines_header)


        def preprocess_line(line):
            columns = line.split(',')

            transaction_date = columns[0]
            customer_id = columns[1]
            product_id = columns[5]
            product_id = product_id.strip('"')
            product_id = product_id.lstrip('0')


            customer_id = customer_id.strip('"')
            transaction_date = transaction_date.strip('"')


            date_parts = transaction_date.split('/')


            year = date_parts[2][2:]

            customer_id = customer_id.lstrip('0')


            modified_date = f"{date_parts[0]}/{date_parts[1]}/{year}"


            new_customer_id = f"{modified_date}-{customer_id}"

            return f"{new_customer_id},{product_id}"

        preprordd = lines.map(preprocess_line)

        header = "DATE-CUSTOMER_ID,PRODUCT_ID\n"

        csvdata = header + '\n'.join(preprordd.collect())

        with open(path_new, 'w+') as f:
            f.write(csvdata)

        return preprordd

    preprocess_tafeng(inputfile, path_new)

    sc = SparkContext.getOrCreate()

    inpRDD = sc.textFile(path_new, 5)
    header = inpRDD.first()

    def frequent(baskets, dist_items, supp, totalnumb):
        candi = []
        i = -1
        while (i < (len(dist_items) - 1)):
            i += 1
            candi.append(tuple([dist_items[i]]))

        freq_sub = {}
        sub_s = int(supp * len(baskets) / totalnumb)
        k = 0
        while candi:
            k += 1
            tmpcount = {}
            i = -1
            while (i < (len(baskets) - 1)):
                i += 1
                basket = baskets[i]
                if len(basket) >= k:
                    j = -1
                    while (j < (len(candi) - 1)):
                        j += 1
                        cand = candi[j]
                        if set(cand).issubset(set(basket)):
                            try:
                                tmpcount[cand] += 1
                            except:
                                tmpcount[cand] = 1
            tmp = []
            for items, count in tmpcount.items():
                if count >= sub_s:
                    tmp.append(items)
            freq_sub[k] = sorted(tmp)

            candi = []
            for indx, x in enumerate(freq_sub[k][:-1]):
                for y in freq_sub[k][indx + 1:]:
                    if x[:-1] == y[:-1]:
                        candi.append(tuple(sorted(list(set(x) | set(y)))))
                    if x[:-1] != y[:-1]:
                        break

        all_sub_freq = []
        for key in freq_sub.keys():
            all_sub_freq.extend(freq_sub[key])

        yield all_sub_freq

    def full_count(basket, candidates):
        res = []
        i = -1
        while (i < (len(candidates) - 1)):
            i += 1
            cand = candidates[i]
            if set(cand).issubset(set(basket)):
                res.extend([(tuple(cand), 1)])
        yield res

    def gen_candi(baskets, distinct_items, supp):
        def gen_candi_k(candidate_k_minus_1):
            candidates = []
            for i in range(len(candidate_k_minus_1)):
                for j in range(i + 1, len(candidate_k_minus_1)):
                    itemset_1 = candidate_k_minus_1[i]
                    itemset_2 = candidate_k_minus_1[j]

                    if itemset_1[:-1] == itemset_2[:-1]:
                        new_candidate = itemset_1 + (itemset_2[-1],)
                        candidates.append(new_candidate)
            return candidates

        def gen_freq_items_k(candidate_k, baskets, supp):
            frequent_itemsets = []
            for candidate in candidate_k:
                count = baskets.filter(lambda basket: set(candidate).issubset(set(basket))).count()
                if count >= supp:
                    frequent_itemsets.append(candidate)
            return frequent_itemsets

        # Initialize variables
        k = 1
        frequent_itemsets = []
        candidate_1 = [(item,) for item in distinct_items]

        while candidate_1:
            frequent_itemsets_k = gen_freq_items_k(candidate_1, baskets, supp)
            frequent_itemsets.extend(frequent_itemsets_k)

            candidate_k_plus_1 = gen_candi_k(frequent_itemsets_k)
            k += 1

            candidate_k_plus_1 = list(set(candidate_k_plus_1))  # Remove duplicates
            candidate_k_plus_1 = sorted(candidate_k_plus_1)  # Sort candidates

            if k > 2:
                candidate_k_plus_1 = rem_candidates(candidate_k_plus_1, frequent_itemsets)

            if not candidate_k_plus_1:
                break

            candidate_1 = candidate_k_plus_1

        return frequent_itemsets

    def rem_candidates(candidate_k_plus_1, frequent_itemsets_k_minus_1):
        removed_candidates = []
        for candidate in candidate_k_plus_1:
            subsets = itertools.combinations(candidate, len(candidate) - 1)
            if all(subset in frequent_itemsets_k_minus_1 for subset in subsets):
                removed_candidates.append(candidate)
        return removed_candidates

    # new
    date_customer_product = inpRDD.map(lambda x: (x.split(',')[0], x.split(',')[1]))
    customer_purchase_count = date_customer_product.countByKey()
    filtered_date_customers = date_customer_product.filter(lambda x: customer_purchase_count[x[0]] > fil_threshold)
    csvRDD = filtered_date_customers.map(lambda x: f'{x[0]},{x[1]}')

    inpRDD = csvRDD.filter(lambda line: line != header).map(lambda line: line.split(',')).map(tuple) \
            .groupByKey().mapValues(set).mapValues(sorted).mapValues(tuple).map(lambda t: (1, t[1])).persist()


    dist_items = inpRDD.flatMap(lambda t: t[1]).distinct().collect()
    # print(dist_items)
    dist_items.sort()

    whole_number = inpRDD.count()
    baskets = inpRDD.map(lambda t: t[1]).persist()
    #print(baskets.take(10))
    
    freq_s = baskets.mapPartitions(lambda part: frequent(list(part), dist_items, supp, whole_number)) \
        .flatMap(lambda x: x).distinct().sortBy(lambda t: (len(t), t)).collect()

    freq_i = baskets.flatMap(lambda basket: full_count(basket, freq_s)).flatMap(lambda x: x).reduceByKey(add) \
        .filter(lambda items: items[1] >= supp).map(lambda items: items[0]).sortBy(lambda t: (len(t), t)).collect()
    candi_set = {}
    for key, group in itertools.groupby(freq_s, lambda items: len(items)):
        candi_set[key] = sorted(list(group), key=lambda x: x)

    freq_set = {}
    for key, group in itertools.groupby(freq_i, lambda items: len(items)):
        freq_set[key] = sorted(list(group), key=lambda x: x)
    l = len(freq_set.keys())

    with open(outputfile, 'w') as f:
        f.write('Candidates:\n')
        output = ''
        for key in candi_set.keys():
            output += str([list(x) for x in candi_set[key]])[1:-1].replace('[', '(').replace(' (', '(').replace(
                ']', ')') + '\n\n'
        f.write(output)

        f.write('Frequent Itemsets:\n')
        output = ''
        i = 1
        for key in freq_set.keys():
            if i < l:
                output += str([list(x) for x in freq_set[key]])[1:-1].replace('[', '(').replace(' (', '(').replace(
                    ']', ')') + '\n\n'
            else:
                output += str([list(x) for x in freq_set[key]])[1:-1].replace('[', '(').replace(' (', '(').replace(
                    ']', ')')
            i += 1
        f.write(output)


if __name__ == '__main__':
    start_time = time.time()
    if len(sys.argv) <= 1:
        sys.exit()
    main(sys.argv)
    print("Duration:", time.time() - start_time)