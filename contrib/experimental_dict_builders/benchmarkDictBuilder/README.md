Benchmarking Dictionary Builder

### Permitted Argument:
Input File/Directory (in=fileName): required; file/directory used to build dictionary; if directory, will operate recursively for files inside directory; can include multiple files/directories, each following "in="

###Running Test:
make test

###Usage:
Benchmark given input files: make ARG= followed by permitted arguments

### Examples:
make ARG="in=../../../lib/dictBuilder in=../../../lib/compress"

###Benchmarking Result:
- First Cover is optimize cover, second Cover uses optimized d and k from first one.
- For every f value of fastCover, the first one is optimize fastCover and the second one uses optimized d and k from first one.
- Fourth column is chosen d and fifth column is chosen k

github:
NODICT       0.000005       2.999642        
RANDOM       0.141553       8.786957        
LEGACY       0.904340       8.989482        
COVER       53.621302       10.641263        8          1298
COVER       4.085037       10.641263        8          1298
FAST15       17.636211       10.586461        8          1778
FAST15       0.221236       10.586461        8          1778
FAST16       18.716259       10.492503        6          1778
FAST16       0.251522       10.492503        6          1778
FAST17       17.614391       10.611737        8          1778
FAST17       0.241011       10.611737        8          1778
FAST18       19.926270       10.621586        8          1778
FAST18       0.287195       10.621586        8          1778
FAST19       19.626808       10.629626        8          1778
FAST19       0.340191       10.629626        8          1778
FAST20       18.918657       10.610308        8          1778
FAST20       0.463307       10.610308        8          1778
FAST21       20.502362       10.625733        8          1778
FAST21       0.638202       10.625733        8          1778
FAST22       22.702695       10.625281        8          1778
FAST22       1.353399       10.625281        8          1778
FAST23       28.041990       10.602342        8          1778
FAST23       3.029502       10.602342        8          1778
FAST24       35.662961       10.603379        8          1778
FAST24       6.524258       10.603379        8          1778

hg-commands:
NODICT       0.000005       2.425291        
RANDOM       0.080469       3.489515        
LEGACY       0.794417       3.911896        
COVER       54.198788       4.131136        8          386
COVER       2.191729       4.131136        8          386
FAST15       11.852793       3.903719        6          1106
FAST15       0.175406       3.903719        6          1106
FAST16       12.863315       4.005077        8          530
FAST16       0.158410       4.005077        8          530
FAST17       11.977917       4.097811        8          818
FAST17       0.162381       4.097811        8          818
FAST18       11.749304       4.136081        8          770
FAST18       0.173242       4.136081        8          770
FAST19       11.905785       4.166021        8          530
FAST19       0.186403       4.166021        8          530
FAST20       13.293999       4.163740        8          482
FAST20       0.241508       4.163740        8          482
FAST21       16.623177       4.157057        8          434
FAST21       0.372647       4.157057        8          434
FAST22       20.918409       4.158195        8          290
FAST22       0.570431       4.158195        8          290
FAST23       21.762805       4.161450        8          434
FAST23       1.162206       4.161450        8          434
FAST24       29.133745       4.159658        8          338
FAST24       3.054376       4.159658        8          338

hg-changelog:
NODICT       0.000006       1.377613        
RANDOM       0.601346       2.096785        
LEGACY       2.544973       2.058273        
COVER       222.639708       2.188654        8          98
COVER       6.072892       2.188654        8          98
FAST15       70.394523       2.127194        8          866
FAST15       0.899766       2.127194        8          866
FAST16       69.845529       2.145401        8          338
FAST16       0.881569       2.145401        8          338
FAST17       69.382431       2.157544        8          194
FAST17       0.943291       2.157544        8          194
FAST18       71.348283       2.173127        8          98
FAST18       1.034765       2.173127        8          98
FAST19       71.380923       2.179527        8          98
FAST19       1.254700       2.179527        8          98
FAST20       72.802714       2.183233        6          98
FAST20       1.368704       2.183233        6          98
FAST21       82.042339       2.180920        8          98
FAST21       2.213864       2.180920        8          98
FAST22       90.666200       2.184297        8          98
FAST22       3.590399       2.184297        8          98
FAST23       108.926377       2.187666        6          98
FAST23       8.723759       2.187666        6          98
FAST24       134.296232       2.189889        6          98
FAST24       19.396532       2.189889        6          98

hg-manifest:
NODICT       0.000005       1.866385        
RANDOM       0.982192       2.309485        
LEGACY       9.507729       2.506775        
COVER       922.742066       2.582597        8          434
COVER       36.500276       2.582597        8          434
FAST15       163.886717       2.377689        8          1682
FAST15       2.107328       2.377689        8          1682
FAST16       152.684592       2.464814        8          1538
FAST16       2.157789       2.464814        8          1538
FAST17       154.463459       2.539834        6          1826
FAST17       2.282455       2.539834        6          1826
FAST18       155.540044       2.576924        8          1922
FAST18       2.101807       2.576924        8          1922
FAST19       152.650343       2.592479        6          290
FAST19       2.359461       2.592479        6          290
FAST20       174.623634       2.594551        8          194
FAST20       2.870022       2.594551        8          194
FAST21       219.876653       2.597128        6          194
FAST21       4.386269       2.597128        6          194
FAST22       247.986803       2.596971        6          386
FAST22       6.201144       2.596971        6          386
FAST23       276.051806       2.601416        8          194
FAST23       11.613477       2.601416        8          194
FAST24       328.234024       2.602830        6          194
FAST24       26.710364       2.602830        6          194
