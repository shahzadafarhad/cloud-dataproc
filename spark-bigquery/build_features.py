# This will help catch some PySpark errors
from py4j.protocol import Py4JJavaError
# A Spark Session is how we interact with Spark SQL to create Dataframes
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType

# Create a SparkSession under the name "reddit". Viewable via the Spark UI
spark = SparkSession.builder.appName("bd4h").getOrCreate()

# Create a two column schema consisting of a string and a long integer
fields = [StructField("ITEMID", StringType(), True),
          StructField("LABEL", StringType(), True)]
schema = StructType(fields)

dictionary = {}

feature_names = ['RBCs', 'WBCs', 'platelets', 'hemoglobin', 'hemocrit',
                 'atypical lymphocytes', 'bands', 'basophils', 'eosinophils', 'neutrophils',
                 'lymphocytes', 'monocytes', 'polymorphonuclear leukocytes',
                 'temperature (F)', 'heart rate', 'respiratory rate', 'systolic', 'diastolic',
                 'pulse oximetry',
                 'troponin', 'HDL', 'LDL', 'BUN', 'INR', 'PTT', 'PT', 'triglycerides', 'creatinine',
                 'glucose', 'sodium', 'potassium', 'chloride', 'bicarbonate',
                 'blood culture', 'urine culture', 'surface culture', 'sputum' +
                 ' culture', 'wound culture', 'Inspired O2 Fraction', 'central venous pressure',
                 'PEEP Set', 'tidal volume', 'anion gap',
                 'daily weight', 'tobacco', 'diabetes', 'history of CV events']

features = ['$^RBC(?! waste)', '$.*wbc(?!.*apache)', '$^platelet(?!.*intake)',
            '$^hemoglobin', '$hematocrit(?!.*Apache)',
            'Differential-Atyps', 'Differential-Bands', 'Differential-Basos', 'Differential-Eos',
            'Differential-Neuts', 'Differential-Lymphs', 'Differential-Monos', 'Differential-Polys',
            'temperature f', 'heart rate', 'respiratory rate', 'systolic', 'diastolic',
            'oxymetry(?! )',
            'troponin', 'HDL', 'LDL', '$^bun(?!.*apache)', 'INR', 'PTT',
            '$^pt\\b(?!.*splint)(?!.*exp)(?!.*leak)(?!.*family)(?!.*eval)(?!.*insp)(?!.*soft)',
            'triglyceride', '$.*creatinine(?!.*apache)',
            '(?<!boost )glucose(?!.*apache).*',
            '$^sodium(?!.*apache)(?!.*bicarb)(?!.*phos)(?!.*ace)(?!.*chlo)(?!.*citrate)(?!.*bar)(?!.*PO)',
            '$.*(?<!penicillin G )(?<!urine )potassium(?!.*apache)',
            '^chloride', 'bicarbonate', 'blood culture', 'urine culture', 'surface culture',
            'sputum culture', 'wound culture', 'Inspired O2 Fraction', '$Central Venous Pressure(?! )',
            'PEEP set', 'tidal volume \(set\)', 'anion gap', 'daily weight', 'tobacco', 'diabetes',
            'CV - past']

patterns = []
for feature in self.features:
    if '$' not in feature:
        patterns.append('.*{0}.*'.format(feature))
    elif '$' in feature:
        patterns.append(feature[1::])

d_items = pd.read_csv(ROOT + 'D_ITEMS.csv', usecols=['ITEMID', 'LABEL'])
d_items.dropna(how='any', axis=0, inplace=True)

script_features_names = ['epoetin', 'warfarin', 'heparin', 'enoxaparin', 'fondaparinux',
                         'asprin', 'ketorolac', 'acetominophen',
                         'insulin', 'glucagon',
                         'potassium', 'calcium gluconate',
                         'fentanyl', 'magensium sulfate',
                         'D5W', 'dextrose',
                         'ranitidine', 'ondansetron', 'pantoprazole', 'metoclopramide',
                         'lisinopril', 'captopril', 'statin',
                         'hydralazine', 'diltiazem',
                         'carvedilol', 'metoprolol', 'labetalol', 'atenolol',
                         'amiodarone', 'digoxin(?!.*fab)',
                         'clopidogrel', 'nitroprusside', 'nitroglycerin',
                         'vasopressin', 'hydrochlorothiazide', 'furosemide',
                         'atropine', 'neostigmine',
                         'levothyroxine',
                         'oxycodone', 'hydromorphone', 'fentanyl citrate',
                         'tacrolimus', 'prednisone',
                         'phenylephrine', 'norepinephrine',
                         'haloperidol', 'phenytoin', 'trazodone', 'levetiracetam',
                         'diazepam', 'clonazepam',
                         'propofol', 'zolpidem', 'midazolam',
                         'albuterol', 'ipratropium',
                         'diphenhydramine',
                         '0.9% Sodium Chloride',
                         'phytonadione',
                         'metronidazole',
                         'cefazolin', 'cefepime', 'vancomycin', 'levofloxacin',
                         'cipfloxacin', 'fluconazole',
                         'meropenem', 'ceftriaxone', 'piperacillin',
                         'ampicillin-sulbactam', 'nafcillin', 'oxacillin',
                         'amoxicillin', 'penicillin', 'SMX-TMP']

script_features = ['epoetin', 'warfarin', 'heparin', 'enoxaparin', 'fondaparinux',
                   'aspirin', 'keterolac', 'acetaminophen',
                   'insulin', 'glucagon',
                   'potassium', 'calcium gluconate',
                   'fentanyl', 'magnesium sulfate',
                   'D5W', 'dextrose',
                   'ranitidine', 'ondansetron', 'pantoprazole', 'metoclopramide',
                   'lisinopril', 'captopril', 'statin',
                   'hydralazine', 'diltiazem',
                   'carvedilol', 'metoprolol', 'labetalol', 'atenolol',
                   'amiodarone', 'digoxin(?!.*fab)',
                   'clopidogrel', 'nitroprusside', 'nitroglycerin',
                   'vasopressin', 'hydrochlorothiazide', 'furosemide',
                   'atropine', 'neostigmine',
                   'levothyroxine',
                   'oxycodone', 'hydromorphone', 'fentanyl citrate',
                   'tacrolimus', 'prednisone',
                   'phenylephrine', 'norepinephrine',
                   'haloperidol', 'phenytoin', 'trazodone', 'levetiracetam',
                   'diazepam', 'clonazepam',
                   'propofol', 'zolpidem', 'midazolam',
                   'albuterol', '^ipratropium',
                   'diphenhydramine(?!.*%)(?!.*cream)(?!.*/)',
                   '^0.9% sodium chloride(?! )',
                   'phytonadione',
                   'metronidazole(?!.*%)(?! desensit)',
                   'cefazolin(?! )', 'cefepime(?! )', 'vancomycin', 'levofloxacin',
                   'cipfloxacin(?!.*ophth)', 'fluconazole(?! desensit)',
                   'meropenem(?! )', 'ceftriaxone(?! desensit)', 'piperacillin',
                   'ampicillin-sulbactam', 'nafcillin', 'oxacillin', 'amoxicillin',
                   'penicillin(?!.*Desen)', 'sulfamethoxazole']

self.script_patterns = ['.*' + feature + '.*' for feature in self.script_features]

# Create an empty DataFrame. We will continuously union our output with this
feature_counts = spark.createDataFrame([], schema)

table = "physionet-data.mimiciii_clinical.d_items"

try:
    table_df = spark.read.format('bigquery').option('table', table).load()
    tables_read.append(table)
except Py4JJavaError:
    pass

# We perform a group-by on subreddit, aggregating by the count and then
# unioning the output to our base dataframe
table_df.show()
