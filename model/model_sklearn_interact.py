#Run in python3.6

import pandas as pd
import numpy as np
from scipy import sparse
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
import math
import getpass
import os

#Model run name
MOD_RUN = 'racecat_moreknots'

INTERACT_VAR = 'majority'

FIXED_EFFECTS = ['dow', 'doy', 'tod', 'fips', 'year', 'statemonth']

FILTER_WEATHER_TERM = True

SENTIMENT_VAR = 'vader'

### Local ###
if getpass.getuser() == 'mattcoop':
    OUT_DIR = '/home/mattcoop/tweets/mod-res/'
    data = pd.read_csv('~/tweets/all_samp_1pct.csv')
    savedat = False

##### Cloud #####
if getpass.getuser() == 'ubuntu':
    OUT_DIR = '/home/ubuntu/tweets/mod-res/'
    data = pd.read_csv('~/tweets/all.csv')
    #data.to_hdf('/home/ubuntu/tweets/data.h5', 'data')
    #savedat = True

if FILTER_WEATHER_TERM:
    data = data[data['weather_term'] != 1] 

#Remove columns we are not using right now
cols = data.columns.tolist()
keepcols = ['precip', 'srad', 'temp.hi'] + FIXED_EFFECTS + [INTERACT_VAR, SENTIMENT_VAR]
dropcols = [c for c in cols if c not in keepcols]

data = data.drop(columns=dropcols)

#Get outcome var and remove from predictors
y = data[[SENTIMENT_VAR]]
data = data.drop(columns=[SENTIMENT_VAR])

#Get categories of interact var
cat = np.sort(data[INTERACT_VAR].unique()).tolist()

# #Set knots
# # Can inspect quantiles with
# # cut = pd.qcut(dat['temp.hi'], q=10)
# # cut.cat.categories
# # Use 20 categories for temp.hi
# # Use 5 for precip (since 85% are 0 anyway)
# # Use 10 for srad (since 50% are 0)
# knots = {'temp.hi': np.linspace(-36, 56, num=15).tolist(),
#         'precip': list(map(lambda x: math.exp(x) - 1, 
#                            np.linspace(math.log(0 + 1), math.log(73 + 1), num=5))),
#         'srad': list(map(lambda x: math.exp(x) - 1,
#                          np.linspace(math.log(0 + 1), math.log(1354 + 1), num=5)))}
knots = {'temp.hi': [data['temp.hi'].min(), -10, 0, 5, 10, 15, 20, 25, 30, 35, data['temp.hi'].max()],
        'precip': [data['precip'].min(), 0.000001, 0.05, 0.5, data['precip'].max()],
        'srad': [data['srad'].min(), 0.000001, 250, 500, 1000, data['srad'].max()]}

#Make predictor matrix with ranges between the knots and with segments of length 10 in the knots
def make_range(l):
    r = []
    for i in range(1, len(l)):
        r = r + np.linspace(l[i-1], l[i], 10, endpoint=False).tolist()
    return(r)

pred = pd.concat([pd.DataFrame({'temp.hi': make_range(knots['temp.hi'])}),
           pd.DataFrame({'precip': make_range(knots['precip'])}),
           pd.DataFrame({'srad': make_range(knots['srad'])})]).fillna(0)

pred = pd.concat([pred]*len(cat))

pred[INTERACT_VAR] = np.repeat(cat, pred.shape[0]/len(cat))

############################################
# Set up accumulator sparse matrices
############################################
predX = sparse.hstack([np.ones([pred.shape[0], 1]), 
                       sparse.csr_matrix(pred[['temp.hi', 'precip', 'srad']].values)])
dataX = sparse.hstack([np.ones([data.shape[0], 1]), 
                       sparse.csr_matrix(data[['temp.hi', 'precip', 'srad']].values)])
labs = ['intercept', 'temp.hi', 'precip', 'srad']

###############################################
#Make continuous segments for predictor vars
###############################################
for grp in cat:
    print(grp)
    for c in knots['precip'][:-1]:
        print('precip', c)
        labs.append('precip_' + grp + '_' + str(c))
        predX = sparse.hstack([predX, sparse.csr_matrix(pd.DataFrame((np.maximum(0, pred['precip'] - c))*(pred[INTERACT_VAR] == grp)))])
        dataX = sparse.hstack([dataX, sparse.csr_matrix(pd.DataFrame((np.maximum(0, data['precip'] - c))*(data[INTERACT_VAR] == grp)))])
    for c in knots['temp.hi'][:-1]:
        print('temp.hi', c)
        labs.append('temp.hi_' + grp + '_' + str(c))
        predX = sparse.hstack([predX, sparse.csr_matrix(pd.DataFrame((np.maximum(0, pred['temp.hi'] - c))*(pred[INTERACT_VAR] == grp)))])
        dataX = sparse.hstack([dataX, sparse.csr_matrix(pd.DataFrame((np.maximum(0, data['temp.hi'] - c))*(data[INTERACT_VAR] == grp)))])
    for c in knots['srad'][:-1]:
        print('srad', c)
        labs.append('srad_' + grp + '_' + str(c))
        predX = sparse.hstack([predX, sparse.csr_matrix(pd.DataFrame((np.maximum(0, pred['srad'] - c))*(pred[INTERACT_VAR] == grp)))])
        dataX = sparse.hstack([dataX, sparse.csr_matrix(pd.DataFrame((np.maximum(0, data['srad'] - c))*(data[INTERACT_VAR] == grp)))])

##################################################
#Make Categorical Vars for interaction categories
##################################################
enc = OneHotEncoder(drop='first', sparse=True)
enccnt = enc.fit(pred.loc[: ,[INTERACT_VAR]])

predX = sparse.hstack([predX, enccnt.transform(pred[[INTERACT_VAR]])])
dataX = sparse.hstack([dataX, enccnt.transform(data[[INTERACT_VAR]])])

labs = labs + enccnt.get_feature_names().tolist()

##############################################
#Make Cateogorical Vars for Control Variables
##############################################

cat_vars = data[FIXED_EFFECTS]
del data # Delete data for memory purposes, not that we have full sparse matrices

enc = OneHotEncoder(drop='first', sparse=True)
enccat = enc.fit(cat_vars)
cat_vars = enccat.transform(cat_vars)

labs = labs + enccat.get_feature_names(FIXED_EFFECTS).tolist()
predX = sparse.hstack([predX, sparse.csr_matrix(np.full([pred.shape[0], cat_vars.shape[1]], 0))])
dataX = sparse.hstack([dataX, cat_vars])

#########################
#Now run regression
#########################
mod = LinearRegression(fit_intercept=False).fit(dataX, y)
coef_res = pd.DataFrame({'names': labs,
                        'coefs': mod.coef_.tolist()[0]})

p = mod.predict(predX)
pred['predicted'] = mod.predict(predX)

#Write results
coef_res.to_csv(OUT_DIR + MOD_RUN + '_coefs.csv', index=False)
pred.to_csv(OUT_DIR + MOD_RUN + '_preds.csv', index=False)

os.system('/home/ubuntu/telegram.sh "End of Model"')

#Get standard errors of coefficients and predictions
#https://stackoverflow.com/questions/22381497/python-scikit-learn-linear-model-parameter-standard-error
#https://otexts.com/fpp2/regression-matrices.html
#https://stats.stackexchange.com/questions/64069/can-we-calculate-the-standard-error-of-prediction-just-based-on-simple-linear-re

y_hat = mod.predict(dataX)
residuals = y - y_hat
N, p = dataX.shape
residual_sum_of_squares = residuals.T @ residuals
sigma_squared_hat = residual_sum_of_squares.iloc[0][0] / (N - p)
XpXinv = sparse.linalg.inv(sparse.csc_matrix(dataX.T @ dataX))
var_beta_hat = XpXinv * sigma_squared_hat

#Getting negatives, cant be right?
diag = var_beta_hat.diagonal()
coef_res['standarderror'] = diag ** 0.5
coef_res.to_csv(OUT_DIR + MOD_RUN + '_coefs.csv', index=False)

predXcsr = predX.tocsr()

pred = pred.reset_index()

for i in range(pred.shape[0]):
    x_star = predXcsr[i,:]
    se = sigma_squared_hat*math.sqrt(1 + (x_star @ XpXinv @ x_star.T)[0,0])
    pred.loc[i, 'se'] = se

pred.to_csv(OUT_DIR + MOD_RUN + '_preds.csv', index=False)

import pickle
pickle.dump(mod, open(OUT_DIR + MOD_RUN + '_mod.sav', 'wb'))

os.system('/home/ubuntu/telegram.sh "End of Script"')
















