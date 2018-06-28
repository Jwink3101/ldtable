from __future__ import unicode_literals,print_function

import numpy as np
import matplotlib.pylab as plt
import matplotlib as mpl


import math
import time
import sys
import copy
import itertools

import ldtable
ldtable = ldtable.ldtable

import json

from collections import OrderedDict

# True will run, save, and exit. False will plot
try:
    run = sys.argv[1].lower() == 'run'
except IndexError:
    run = False

def get_dict(i):
    return OrderedDict([('i',i),('i2',2*i),('ihd',0.5*i),('iri',int(1.0*i**(0.5)))])

def yield_items(N):
    for ii in xrange(N):
        yield get_dict(ii)

######################
def time_dataset_mem(index=False):
    import dataset
    try:
        del DF
    except:
        pass

    T0 = time.time()
    db = dataset.connect('sqlite:///:memory:')
    table = db['test_table']
    table.insert_many(yield_items(Nd))
    
    if index:
        table.create_index(['iri'])
    
    TC = time.time() - T0
    
    T0 = time.time()
    L = len(list(table.find(iri=30)))
    TQ = time.time() - T0
    
    sys.stdout.write('-{}-'.format(L));sys.stdout.flush()
    return TC,TQ


def time_pandas():
    import pandas as pd
    try:
        del DF
    except:
        pass

    T0 = time.time()
    DF = pd.DataFrame(yield_items(Nd))
    TC = time.time()-T0

    T0 = time.time()
    L = len(DF[DF.iri == 30])
    TQ = time.time() - T0
    
    sys.stdout.write('-{}-'.format(L));sys.stdout.flush()
    return TC,TQ

def time_ldtable():
    try:
        del DB
    except:
        pass

    T0 = time.time()
    DB = ldtable(yield_items(Nd))
    TC = time.time()-T0

    T0 = time.time()
    L = len(DB[DB.Q.iri == 30])
    TQ = time.time() - T0
    
    sys.stdout.write('-{}-'.format(L));sys.stdout.flush()
    return TC,TQ

def time_tinyDBmem():
    from tinydb import TinyDB,where
    from tinydb.storages import MemoryStorage

    try:
        del tDB
    except:
        pass

    T0 = time.time()
    tDB = TinyDB(storage=MemoryStorage)
    tDB.insert_multiple(yield_items(Nd))
    TC = time.time()-T0

    T0 = time.time()
    L = len(tDB.search(where('iri')==30))
    TQ = time.time() - T0
    sys.stdout.write('-{}-'.format(L));sys.stdout.flush()
    return TC,TQ
    

def time_loop_copy():
        
    T0 = time.time()
    list_data = list(yield_items(Nd))
    TC = time.time()-T0

    T0 = time.time()
    L = len([item for item in list_data if item['iri']==30])
    TQ = time.time() - T0
    
    sys.stdout.write('-{}-'.format(L));sys.stdout.flush()
    return TC,TQ

def time_sqlitemem(index=False):
    import sqlite3
    try:
        del conn
    except:
        pass

    T0 = time.time()
    
    conn = sqlite3.connect(':memory:')
    # conn.row_factory = sqlite3.Row
    #conn.row_factory = OrderedDict_factory

    cursor = conn.cursor()
    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS tab 
        (i int,i2 int,ihd real,iri int)""")
    conn.commit()
    
    cursor = conn.cursor()
    dd = ( list(d.values()) for d in yield_items(Nd))
    cursor.executemany("INSERT INTO tab VALUES (?,?,?,?)",dd)
    conn.commit()
    
    if index:
        """ 
        Create an index. We will do this AFTER since it is faster.
        A similar comparison is made with others
        """
        cursor = conn.cursor()
        cursor.execute("""\
            CREATE INDEX index_iri
            ON tab (iri)""")
        conn.commit()
    
    TC = time.time()-T0

    T0 = time.time()
    cursor = conn.cursor()
    cursor.execute('SELECT * from tab where iri=?',(30,))
    L = len(list(cursor))
    TQ = time.time() - T0
    
    sys.stdout.write('-{}-'.format(L));sys.stdout.flush()
    
    return TC,TQ
######################
    
def compute_averages(arr,name=''):
    mTC = sum(a[0] for a in arr)*1.0/len(arr)
    print('\n{:s} Create Avg (N={:d}): {:0.5e}'.format(name,len(arr),mTC))

    mTQ = sum(a[1] for a in arr)*1.0/len(arr)
    print('{:s} Query Avg (N={:d}): {:0.5e}'.format(name,len(arr),mTQ))
    return {'TC':mTC,'TQ':mTQ}


def test(N=10):
    results = OrderedDict()
    results['pandas'] = compute_averages([time_pandas() for _ in xrange(N)],name='Pandas')
    results['ldtable'] = compute_averages([time_ldtable() for _ in xrange(N)],name='ldtable')
    results['TinyDB_mem'] = compute_averages([time_tinyDBmem() for _ in xrange(N)],name='tinyDB in memory')
    results['dataset_mem'] = compute_averages([time_dataset_mem() for _ in xrange(N)],name='dataset_mem')
    results['dataset_mem_index'] = compute_averages([time_dataset_mem(index=True) for _ in xrange(N)],name='dataset_mem_index')
    results['loop_copy'] = compute_averages([time_loop_copy() for _ in xrange(N)],name='loop_copy')
    results['sqlite'] = compute_averages([time_sqlitemem(False) for _ in xrange(N)],name='sqlite')
    results['sqlite_indx'] = compute_averages([time_sqlitemem(True) for _ in xrange(N)],name='sqlite_index')
    
    return results

Nds = [int(10.0**(a)) for a in [2,2.5,3,3.5,4,4.5,5,5.5,6]]
#Nds = [int(10.0**(a)) for a in [2,2.5,3,3.5]]
#Nds = [int(10.0**(a)) for a in [4]]



if run:
    ###############################
    all_res = []
    for Nd in Nds:
        print(math.log10(Nd))
        
        # data will be generated in each function 
        
        all_res.append(test())
        print("")


    with open('results.json','w') as F:
        json.dump(all_res,F)
    
    sys.exit()
    #############################

with open('results.json') as FF:
    all_res = json.load(FF,object_pairs_hook=OrderedDict)

plt.close('all')
plt.style.use('seaborn-darkgrid')
fig = plt.figure(figsize=(15,4))

axes = [None for _ in range(3)]
axes[0] = plt.subplot2grid((1,5), (0,0), colspan=2)
axes[1] = plt.subplot2grid((1,5), (0,2), colspan=2)
axes[2] = plt.subplot2grid((1,5), (0,4), colspan=1)


shapes = itertools.cycle(['-o', '-s', '-D', '-<', '-H'])


mpl.rcParams['font.size'] = 14
mpl.rcParams['lines.linewidth'] = 2

methods = all_res[0].keys()
for it,tool in enumerate(methods):
    shape = next(shapes)
    axes[0].plot(Nds,[R[tool]['TQ'] for R in all_res],shape)
    axes[1].plot(Nds,[R[tool]['TC'] for R in all_res],shape)
    
    axes[2].plot([],[],shape,label=methods[it])
    
    print([R[tool]['TQ'] for R in all_res])
    
for ax in axes[:2]:
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('database size')
    ax.set_ylim([1e-6,1e2])
    ax.set_xlim([10.0**(1.9),10.0**(6.1)])
#     ax.set_aspect('equal',adjustable='box')

axes[1].set_yticklabels([])

axes[0].set_title('Query')
axes[1].set_title('Create')

axes[0].set_ylabel('time (s)')

ax = axes[2]
ax.legend(loc='center',numpoints=1,fontsize=12)
ax.set_frame_on(False)
ax.set_xticks([])
ax.set_yticks([])

fig.tight_layout()
fig.savefig('benchmark.png')
#plt.show()


# Slope at the end
slopeQ = {}
slopeC = {}
for tool in methods:
    Qtime = np.log10([res[tool]['TQ'] for res in all_res[-2:]])
    Qtime = Qtime[-1]-Qtime[-2]
    
    slopeQ[tool] = Qtime/0.5
    
    Ctime = np.log10([res[tool]['TC'] for res in all_res[-2:]])
    Ctime = Ctime[-1]-Ctime[-2]
    
    slopeC[tool] = Ctime/0.5
    
    print('{}|{:0.2f}|{:0.2f}'.format(tool,slopeQ[tool],slopeC[tool]))







