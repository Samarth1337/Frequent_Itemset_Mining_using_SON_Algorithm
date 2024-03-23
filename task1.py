from pyspark import SparkContext
import itertools
from itertools import combinations
import collections
import time
from functools import reduce
from operator import add
import sys

def main(argv):

    case_numb=int(sys.argv[1])
    sup=int(sys.argv[2])
    inputfile=sys.argv[3]
    outputfile=sys.argv[4]

    sc=SparkContext.getOrCreate()
    dataRDD=sc.textFile(inputfile, 2)
    heade=dataRDD.first()

    def frequent(baskets, distinctitems, sup, totalnum):
        candi=[]
        i=-1
        while(i < (len(distinctitems)-1)):
            i += 1
            candi.append(tuple([distinctitems[i]]))

        freq_sub={}
        subs=int(sup * len(baskets) / totalnum)
        k=0
        while candi:
            k += 1
            tmp_count={}
            i=-1
            while(i < (len(baskets)-1)):
                i += 1
                basket=baskets[i]
                if len(basket) >= k:
                    j=-1
                    while(j < (len(candi)-1)):
                        j += 1
                        cand=candi[j]
                        if set(cand).issubset(set(basket)):
                            try: tmp_count[cand] += 1
                            except: tmp_count[cand]=1
            tmp=[]
            for items, count in tmp_count.items():
                if count >= subs:
                    tmp.append(items)
            freq_sub[k]=sorted(tmp)

            candi=[]
            for indx, x in enumerate(freq_sub[k][:-1]):
                for y in freq_sub[k][indx+1:]:
                    if x[:-1] == y[:-1]:
                        candi.append(tuple(sorted(list(set(x) | set(y)))))
                    if x[:-1] != y[:-1]:
                        break

        all_sub_freq=[]
        for key in freq_sub.keys():
            all_sub_freq.extend(freq_sub[key])

        yield all_sub_freq

    def full_count(basket, candidates):
        res=[]
        i=-1
        while(i < (len(candidates)-1)):
            i += 1
            cand=candidates[i]
            if set(cand).issubset(set(basket)):
                res.extend([(tuple(cand), 1)])
        yield res

    def gen_candi(baskets, distinct_items, sup):
        def gen_candi_k(candidate_k_minus_1):
            candidates=[]
            for i in range(len(candidate_k_minus_1)):
                for j in range(i + 1, len(candidate_k_minus_1)):
                    item_s_1=candidate_k_minus_1[i]
                    item_s_2=candidate_k_minus_1[j]

                    if item_s_1[:-1] == item_s_2[:-1]:
                        new_candidate=item_s_1 + (item_s_2[-1],)
                        candidates.append(new_candidate)
            return candidates

        def gen_freq_item_k(candidate_k, baskets, sup):
            frequent_itemsets=[]
            for candidate in candidate_k:
                count=baskets.filter(lambda basket: set(candidate).issubset(set(basket))).count()
                if count >= sup:
                    frequent_itemsets.append(candidate)
            return frequent_itemsets

        # Initialize variables
        k=1
        frequent_itemsets=[]
        candidate_1=[(item,) for item in distinct_items]

        while candidate_1:
            frequent_itemsets_k=gen_freq_item_k(candidate_1, baskets, sup)
            frequent_itemsets.extend(frequent_itemsets_k)

            candidate_k_plus_1=gen_candi_k(frequent_itemsets_k)
            k += 1

            candidate_k_plus_1=list(set(candidate_k_plus_1))  # Remove duplicates
            candidate_k_plus_1=sorted(candidate_k_plus_1)  # Sort candidates

            if k > 2:
                candidate_k_plus_1=rem_candi(candidate_k_plus_1, frequent_itemsets)

            if not candidate_k_plus_1:
                break

            candidate_1=candidate_k_plus_1

        return frequent_itemsets

    def rem_candi(candidate_k_plus_1, frequent_itemsets_k_minus_1):
        removed_candidates=[]
        for candidate in candidate_k_plus_1:
            subsets=itertools.combinations(candidate, len(candidate) - 1)
            if all(subset in frequent_itemsets_k_minus_1 for subset in subsets):
                removed_candidates.append(candidate)
        return removed_candidates

    if case_numb == 1:
        inp_RDD=dataRDD.filter(lambda line: line != heade).map(lambda line: line.split(',')).map(tuple)\
            .groupByKey().mapValues(set).mapValues(sorted).mapValues(tuple).map(lambda t: (1, t[1])).persist()
    elif case_numb == 2:
        inp_RDD=dataRDD.filter(lambda line: line != heade).map(lambda line: line.split(',')).map(lambda t: (t[1], t[0]))\
            .groupByKey().mapValues(set).mapValues(sorted).mapValues(tuple).map(lambda t: (1, t[1])).persist()

    distinctitems=inp_RDD.flatMap(lambda t: t[1]).distinct().collect()
    distinctitems.sort()

    wh=inp_RDD.count()
    baskets=inp_RDD.map(lambda t: t[1]).persist()

    freq_s=baskets.mapPartitions(lambda part: frequent(list(part), distinctitems, sup, wh))\
        .flatMap(lambda x: x).distinct().sortBy(lambda t: (len(t), t)).collect()

    freq_i=baskets.flatMap(lambda basket: full_count(basket, freq_s)).flatMap(lambda x: x).reduceByKey(add)\
        .filter(lambda items: items[1] >= sup).map(lambda items: items[0]).sortBy(lambda t: (len(t), t)).collect()

    candi_set={}
    for key, group in itertools.groupby(freq_s, lambda items: len(items)):
        candi_set[key]=sorted(list(group), key=lambda x: x)

    freq_set={}
    for key, group in itertools.groupby(freq_i, lambda items: len(items)):
        freq_set[key]=sorted(list(group), key=lambda x: x)
    l=len(freq_set.keys())

    with open(outputfile, 'w') as f:
        f.write('Candidates:\n')
        output=''
        for key in candi_set.keys():
            output += str([list(x) for x in candi_set[key]])[1:-1].replace('[', '(').replace(' (', '(').replace(']', ')')+'\n\n'
        f.write(output)

        f.write('Frequent Itemsets:\n')
        output=''
        i=1
        for key in freq_set.keys():
            if i < l:
                output += str([list(x) for x in freq_set[key]])[1:-1].replace('[', '(').replace(' (', '(').replace(']', ')')+'\n\n'
            else:
                output += str([list(x) for x in freq_set[key]])[1:-1].replace('[', '(').replace(' (', '(').replace(']', ')')
            i += 1
        f.write(output)

if __name__ == '__main__':
    start_time=time.time()
    if len(sys.argv) <= 1:
        sys.exit()
    main(sys.argv)
    print("Duration:", time.time() - start_time)