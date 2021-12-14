import string
import re    # for simple regulars
import regex # for diacritics characters processing with \p{Mn} (sorry re)
from collections import OrderedDict #we need to preserve order of phrases to clean, but native dict preserve order of keys only from python 3.7 (google colab uses 3.6, sorry)
import unicodedata
from geotext import GeoText
import geonamescache
import nltk
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer

from collections import Counter
import math

from gainy_compute.industries.prototype.ahocorasick import textclean_ahocorasick_createnode, \
    textclean_ahocorasick_processtext

nltk.download('stopwords')

def textclean_createtextstoremove()-> \
        (OrderedDict, #textclean_phrases_dict_1:OrderedDict, #case-sensitive: (replacings to whitespaces) continents,countries,us-states-long,us-states-iso,us-states-iso3,cities, special cases phrases/words (founded and formulated manually)
         OrderedDict, #textclean_phrases_dict_2:OrderedDict, #case-lowered: (replacings to phrases) special cases phrases/words (founded and formulated manually)
         OrderedDict  #textclean_phrases_dict_3:OrderedDict  #case-lowered: (replacings to whitespaces) nationalities, continents, countries, us-states, usual english stop-words, special cases words (founded and formulated manually)
         ):
    #######################################
    ## textclean_phrases_dict_1:dict
    ## case-sensitive: (replacings to whitespaces) continents, countries, us-states, us-states iso, us-states iso3, cities, special cases phrases/words (founded and formulated manually)
    #######################################
    list_prep = []

    gnc = geonamescache.GeonamesCache()

    #continents
    geo_collection = gnc.get_continents()
    #continents - long names
    list_prep += [an_n['name'] for c in geo_collection for an_n in geo_collection[c]['alternateNames'] if an_n['lang']=='en']
    #continents - codes
    list_prep += list(geo_collection.keys())

    #countries
    geo_collection = gnc.get_countries()
    #countries - long names
    list_prep += [geo_collection[c]['name'] for c in geo_collection]
    #countries - iso
    list_prep += [geo_collection[c]['iso'] for c in geo_collection]
    #countries - iso3
    list_prep += [geo_collection[c]['iso3'] for c in geo_collection]

    #cities, all kind of (only english variants, coz there are woudld be millions...)
    geo_collection = gnc.get_cities()
    list_prep += [n for c in geo_collection for n in geo_collection[c]['alternatenames']
                  if (len(n)<=50)and(re.search("[^a-z "+string.punctuation+"0-9A-Z_]", n)==None)] #only international symbols (latin,num,puncttns), limit name len

    #US states
    geo_collection = gnc.get_us_states()
    list_prep += [geo_collection[c]['name'] for c in geo_collection]

    #specials
    list_prep +=["People's Republic of China", "Greater China", "Continental", "Pacific", "Latin", "Middle East/Africa", "East/Africa"]

    # all sorted, no duplicates(set) (sorted list, so as not to remove partially from longer phrase-cases, will go from longer to shorter)
    list_prep = sorted(filter(lambda x: len(x)>0,
                              set(list_prep)),
                       key=lambda x: (-len(x),x), reverse=False)

    textclean_phrases_dict_1 = OrderedDict.fromkeys(list_prep, " ")


    #######################################
    ## textclean_phrases_dict_2:OrderedDict
    ## case-lowered: (replacings to phrases) special cases phrases/words (founded and formulated manually)
    #######################################

    raw_1 = [ 'business',  'businesses',  'companies',  'companys',  'corporation',  'inc',  'lp',  'ltd', 'company', \
              'design',  'designs',  'development',  'develops',  'distributes',  'distribution',  'electric',  'electrical',  'engineered',  'engineering', \
              'include',  'including',  'industrial',  'industries',  'industry',  'international',  'internationally',  'worldwide', \
              'manufacturers',  'manufactures',  'manufacturing',  'marketing',  'markets',  'operated',  'operates',  'operations',  'owned',  'owns', \
              'produces',  'production',  'product',  'products',  'service',  'services',  'supplies',  'supply',  'system',  'systems', \
              'technologies',  'technology',  'transport',  'transportation',  'transports',  'use',  'used',  'vehicle',  'vehicles']
    replacement_1 = ['business', 'business', 'company', 'company', 'company', 'company', 'company', 'company', 'company', \
                     'design', 'design', 'develop', 'develop', 'distribute', 'distribute', 'electric', 'electric', 'engineer', 'engineer', \
                     'include', 'include', 'industry', 'industry', 'industry', 'international', 'international', 'international', \
                     'manufacture', 'manufacture', 'manufacture', 'market', 'market', 'operate', 'operate', 'operate', 'own', 'own', \
                     'produce', 'produce', 'product', 'product', 'service', 'service', 'supply', 'supply', 'system', 'system', \
                     'technology', 'technology', 'transport', 'transport', 'transport', 'use', 'use', 'vehicle', 'vehicle']

    raw_2 = [ 'business',  'businesses',  'companies',  'companys',  'corporation',  'inc',  'lp',  'ltd', 'firm', \
              'design',  'designs',  'development',  'develops',  'distributes',  'distribution',  'electric',  'electrical',  'engineered',  'engineering', \
              'include',  'including',  'industrial',  'industries',  'industry',  'international',  'internationally',  'worldwide', \
              'manufacturers',  'manufactures',  'manufacturing',  'marketing',  'markets',  'operated',  'operates',  'operations',  'owned',  'owns', \
              'produces',  'production',  'product',  'products',  'service',  'services',  'supplies',  'supply',  'system',  'systems', \
              'technologies',  'technology',  'transport',  'transportation',  'transports',  'use',  'used',  'vehicle',  'vehicles']
    replacement_2 = ['business', 'business', 'company', 'company', 'company', 'company', 'company', 'company', 'company', \
                     'design', 'design', 'develop', 'develop', 'distribute', 'distribute', 'electric', 'electric', 'engineer', 'engineer', \
                     'include', 'include', 'industry', 'industry', 'industry', 'international', 'international', 'international', \
                     'manufacture', 'manufacture', 'manufacture', 'market', 'market', 'operate', 'operate', 'operate', 'own', 'own', \
                     'produce', 'produce', 'product', 'product', 'service', 'service', 'supply', 'supply', 'system', 'system', \
                     'technology', 'technology', 'transport', 'transport', 'transport', 'use', 'use', 'vehicle', 'vehicle']

    raw_3 = [ 'business',  'businesses',  'companies',  'companys',  'corporation',  'inc',  'lp',  'ltd', 'company', \
              'design',  'designs',  'development',  'develops',  'distributes',  'distribution', 'technologies',  'technology', \
              'include',  'including',  'industrial',  'industries',  'industry',  'international',  'internationally',  'worldwide', \
              'manufacturers',  'manufactures',  'manufacturing',  'marketing',  'markets',  'operated',  'operates',  'operations',  'owned',  'owns', \
              'produces',  'production',  'product',  'products',  'service',  'services',  'supplies',  'supply',  'system',  'systems', \
              # mega
              'united kingdom', 'united states', 'peoples republic china', 'north america', 'agribusiness', 'alkaline88', 'aquariumbased', \
              'asia pacific', 'b', 'b122', 'biotechnology', 'cheeseflavored', 'companyowned', 'customerspecific', 'etc', 'europeanstyle', \
              'fruitbased', 'greenhousegrown', 'healthconscious', 'healthrelated', 'hempderived', 'hormonefree', 'legumebased', 'licenseeowned', \
              'nutrientrich', 'oilroasted', 'patentpending', 'probioticbased', 'proteinfiber', 'relapsedrefractory', 'scientificallyvalidated', \
              'selling', 'serves', 'statechartered', 'upanddownthestreet', 'vegetablebased', 'visionspecific', 'waterbased', 'weightmanagement',
              'co', 'plc', \
              #mega
              'biopharmaceutical','diseases','pharmaceuticals','iii','pharmaceutical','ii','pharma','diagnostics','diagnosis','biopharma','iib','equipment', \
              'inhibitors','ib','delivers','2a','1b','inhibits','independent','receptors','deliver','iv','2b','iia','rheumatoid','pharmacies','delivering', \
              '3d','iiia','dependent','biopharmaceuticals','traumatic','dependence','iiiii','1b2','12a','hemoglobin','diagnose','intranasal','ibii','igg1', \
              'enzymes','intratumoral','delivered','cyclindependent','hemoglobinuria','dentists','biotechnologies','select','implantation','phases','psoriatic', \
              'nk','inducible','seattle','abuse','cannabinoids','quantitative','employee','neoantigen','diagnosed','contraceptive','patch','devastating', \
              'cytomegalovirus','adenovirus','pnh','managed','goods','hempbased','alcoholic','beverages','inhibition','placebocontrolled','dehydrogenase', \
              'posttraumatic','pharmacology','iibiii','ia','antitumor','tumorspecific','ill','illness','illnesses','iaib','pharmacological','bioidentical', \
              'tumorinduced','il12','pharmacokinetic','1b2a','dentistry','tumorderived','iga','41bb','11b','drugdevice','injectors','igm','purine','receptorlike', \
              'drugdelivery','2negative','transfusiondependent','pharmacists','ig','il2','infectioncontrol','intravaginal','immunoinflammatory','interface', \
              'insulindependent','receptorpositive','igg4','dialysisdependent','neurodiagnostic','immunoneurologyimmunooncology','pharmaceutica','enzymecdg', \
              '1ab','5t4','biodelivery','hormonereceptor','liverrelated','5a','1programmed','independently','1a1b','receptor1','rheumatology','tumorgraft', \
              'receptort','devicespecific','rpharm','2phase','methioninedependent','fox','chase','queens','michigan','establishment','a4Гџ7specific','pharmatech', \
              'pharmacogeneticallytargeted','subcutaneouslydelivered','sopharma','16positive','immunoreceptor','tumorassociated','2d','pharmacodynamic', \
              'tumordependent','1a','folatereceptor','imgn632','pellepharm','diagnoses','karyopharm','pharmacologically','orallydelivered','carboxyhemoglobin', \
              'methemoglobin','mustang','liverdirected','receptorГџ','pharmacovigilance','2b3','nanopharma','1cbetaklotho','liverfocused','qsidental','oxytocin', \
              '1life','4kscore','3b','37s','5based','3m','biomarkerdiagnostic','scpharmaceuticals','nondiagnostic','pharmacodynamics','deliverability','injector', \
              'receptoralpha','enzymatic','1associated','intratumorally','300unit','10x','enzymelinked','receptorsubtype','trГџ','vk2809','liverselective', \
              'pharmsynthez','transnasal','readytouse','xeris','infusible','xerisol','xeriject','highlyconcentrated','commercially','autoinjectors','multidose', \
              'gvoke','hypopen','x4','mavorixafor','rear','cxcr4','hypogammaglobulinemia','myelokathexis','waldenstrГ¶m','x4p002','x4p003','avise','exagen','sle', \
              'c4d','ra','bound','aps','ahn','cellbound','rheumatologists','prognosis','ctd','differential','presenting','indicative','ctds','acceleron', \
              'luspaterceptaamt','betathalassemia','reblozyl','erythroid','maturation','sotatercept','ace1334','nontransfusiondependent','lowerrisk','salk', \
              'obexelimab','igg4related','plamotamab','tumortargeted','xmab717','xmab841','xmab104','xoma','aggregator','pth1r','antiparathyroid', \
              'hyperparathyroidism','hypercalcemia','xmeta','receptoractivating','x213','dentsply','sirona','endodontic','sealants','whiteners','fluoride','curing', \
              'scalers','polishers','precious','metal','alloys','crown','xtl','hcdr1','iiready','yeda','xenograft','ramat','gan','ymabs','danyelza','leptomeningeal', \
              'firstline','thirdline','gd2gd3','omburtamab','murine','yield10','agricultural','stepchange','increases','crop','yield','trait','factory','oilseed', \
              'camelina','gdm','seeds','j','r','simplot','forage','metabolix','woburn','dasiglucagon','lixisenatide','adlyxin','lyxumia','soliqua','suliqua', \
              'autoinjector','dualhormone','zogenix','fintepla','fenfluramine','mt1621','thymidine','tevard','ziopharm','sleeping','transposontransposase','regular', \
              'manner','mainland','macau','zejula','odronextamab','zentalis','cayman','znc5','znc3','wee1','zosano','bioactive','microneedle','qtrypta','m207', \
              'zolmitriptan','zynerba','pharmaceuticallyproduced','nearrare','improves','22q112','deletion','heterogeneous','epilepsies','developmental', \
              'encephalopathies','alltranz','devon','zynex','nmes','volume','nexwave','interferential','transcutaneous','tens','neuromove','electromyography', \
              'triggered','inwave','ewave','batteries','electrotherapy','comfortracsaunders','mosquitos','codiagnostics','reaction','multiplexed','carried','genomes', \
              'cogent','cgt9486','d816v','drives','plexxikon','cgt0206','unum','collegium','xtampza','nucynta','abusedeterrent','oxycodone','ir','tapentadol','enough', \
              'require','aroundtheclock','stoughton'
              ]
    replacement_3 = ['business', 'business', 'company', 'company', 'company', 'company', 'company', 'company', 'company', \
                     'design', 'design', 'develop', 'develop', 'distribute', 'distribute', 'technology', 'technology', \
                     'include', 'include', 'industry', 'industry', 'industry', 'international', 'international', 'international', \
                     'manufacture', 'manufacture', 'manufacture', 'market', 'market', 'operate', 'operate', 'operate', 'own', 'own', \
                     'produce', 'produce', 'product', 'product', 'service', 'service', 'supply', 'supply', 'system', 'system', \
                     # mega
                     'uk', 'us', 'china', 'us', 'agriculture', ' ', 'aquarium based', ' ', ' ', ' ', 'biotech', 'cheese flavored', \
                     'company owned', 'customerspecific', ' ', 'european style', 'fruit based', 'greenhouse grown', 'health conscious', \
                     'health related', 'hemp derived', 'hormone free', 'legume based', 'licensee owned', 'nutrient rich', 'oil roasted', \
                     'patent pending', 'probiotic based', 'protein fiber', 'relapsed refractory', 'scientifically validated', 'selling', \
                     'serves', 'state chartered', 'up and down the street', 'vegetable based', 'vision specific', 'water based', \
                     'weight management',
                     'company', 'company', \
                     #mega
                     'pharmacy bio','disease','pharmacy',' ','pharmacy',' ','pharmacy','diagnostic','diagnostic','pharmacy bio',' ','device','inhibitor',' ', \
                     'delivery',' ',' ','inhibitor','independence','receptor','delivery',' ',' ',' ','rheumatic arthritis','pharmacy','delivery',' ',' ', \
                     'dependence','pharmacy bio','trauma','dependence',' ',' ',' ','hemoglobin blood','diagnostic','nasal',' ',' ','enzyme','tumor','delivery', \
                     'cyclin dependence','hemoglobin blood urine','dental','biotech',' ','implant','phase','psoriase disease',' ',' ',' ',' ','drug',' ',' ', \
                     'gene neoantigen','diagnostic','contraception',' ',' ','virus cytomegalovirus','virus adenovirus',' ','manage',' ','cannabis drug','alcohol', \
                     ' ','inhibitor receptor','placebo study','dehydrogenase ','trauma','pharmacy',' ',' ','tumor','tumor','disease','disease','disease',' ', \
                     'pharmacy','bio','tumor induced',' ','pharmacy',' ','dental','tumor derived',' ',' ',' ','drug device','device',' ','purine chemical compound', \
                     'receptor','delivery drug',' ','transfucion dependence','pharmacy',' ',' ','infection control','vaginal','immune','tech','insulin dependence', \
                     'receptor',' ','dialysis dependence','neuro diagnostic','immune cancer','pharmacy','enzyme',' ',' ','delivery bio','hormone receptor','liver', \
                     ' ',' ','independence',' ','receptor','rheumatic','tumor','receptor','device','pharmacy',' ','methionine dependence','animal',' ',' ',' ',' ', \
                     ' ','pharmacy tech','pharmacy gene','delivery','pharmacy',' ','immune receptor','tumor',' ','pharmacy','tumor',' ','receptor','cancer','pharmacy', \
                     'diagnostic','pharmacy','pharmacy','delivery oral','hemoglobin blood carboxy therapy drug','hemoglobin blood disease','animal','liver', \
                     'receptor','pharmacy',' ','pharmacy tech',' ','liver','dental','oxytocin hormone',' ',' ',' ',' ',' ',' ','bio marker diagnostic','pharmacy', \
                     'diagnostic','pharmacy','delivery','device','receptor','enzyme',' ','tumor',' ',' ','enzyme','receptor',' ',' ','liver','pharmacy','nasal',' ', \
                     'company',' ','device','device',' ',' ','device',' ','device','device',' ','drug',' ','gene receptor','blood disorder','blood disorder',' ',' ', \
                     ' ','blood test diagnostics',' ','company',' ','company',' ','company','company','cell','rheumatic',' ','disease',' ',' ',' ','disease',' ', \
                     'drug anemia disorder','beta thalassemia inherited blood disorder disease hemoglobin','drug anemia disorder','blood',' ','protein drug', \
                     'protein drug','disease',' ','biological research institute','treatment','Immune disease','antibody','tumor','hormone antibody','hormone antibody', \
                     'hormone antibody','biotech',' ','hormone receptor gene','hormone antibody','disease','disease',' ','receptor',' ','dental',' ','dental','dental', \
                     'dental',' ',' ',' ',' ',' ',' ',' ',' ','pharmacy','drug',' ','pharmacy research study','transplantation',' ',' ','bio therapy cancer', \
                     'drug cancer','cancer disease',' ',' ','gene cancer','therapy','animal',' ','nutrition food agriculture',' ',' ','nutrition food agriculture',' ', \
                     'gene','production','nutrition food agriculture','nutrition food agriculture','diabetes treatment','nutrition food agriculture',' ',' ', \
                     'nutrition food agriculture','nutrition food agriculture','bio science',' ','drug','drug','drug','drug','drug','drug','device','hormone',' ', \
                     'drug','drug','gene therapy','drug','bio science','pharmacy',' ','dna',' ',' ',' ',' ','drug','drug','pharmacy',' ','drug','drug','protein', \
                     'pharmacy','bio active','device','device','drug','drug','pharmacy production','pharmacy production',' ',' ','disease','processing',' ','disease', \
                     'dna gene','disease','pharmacy',' ','device','device',' ','device',' ',' ',' ',' ','diagnostics',' ',' ','device','device','electrical therapy', \
                     'device','insect','diagnostics',' ',' ',' ','gene',' ','drug',' ',' ',' ','drug',' ',' ','drug','drug','abuse deterrent','drug',' ','drug', \
                     ' ',' ',' ',' '
                     ]

    raw_4 = [ 'business',  'businesses',  'companies',  'companys',  'corporation',  'inc',  'lp',  'ltd', 'company', \
              'design',  'designs',  'development',  'develops',  'distributes',  'distribution', 'technologies',  'technology', \
              'include',  'including',  'industrial',  'industries',  'industry',  'international',  'internationally',  'worldwide', \
              'manufacturers',  'manufactures',  'manufacturing',  'marketing',  'markets',  'operated',  'operates',  'operations',  'owned',  'owns', \
              'produces',  'production',  'product',  'products',  'service',  'services',  'supplies',  'supply',  'system',  'systems']
    replacement_4 = ['business', 'business', 'company', 'company', 'company', 'company', 'company', 'company', 'company', \
                     'design', 'design', 'develop', 'develop', 'distribute', 'distribute', 'technology', 'technology', \
                     'include', 'include', 'industry', 'industry', 'industry', 'international', 'international', 'international', \
                     'manufacture', 'manufacture', 'manufacture', 'market', 'market', 'operate', 'operate', 'operate', 'own', 'own', \
                     'produce', 'produce', 'product', 'product', 'service', 'service', 'supply', 'supply', 'system', 'system']

    raw_5 = [ 'business',  'businesses',  'companies',  'companys',  'corporation',  'inc',  'lp',  'ltd', 'company', \
              'design',  'designs',  'development',  'develops',  'distributes',  'distribution', 'technologies',  'technology', \
              'include',  'including',  'industrial',  'industries',  'industry',  'international',  'internationally',  'worldwide', \
              'manufacturers',  'manufactures',  'manufacturing',  'marketing',  'markets',  'operated',  'operates',  'operations',  'owned',  'owns', \
              'produces',  'production',  'product',  'products',  'service',  'services',  'supplies',  'supply',  'system',  'systems', \
              # mega
              'united kingdom', 'united states', 'peoples republic china', 'north america', 'agribusiness', 'alkaline88', 'aquariumbased', \
              'asia pacific', 'b', 'b122', 'biotechnology', 'cheeseflavored', 'companyowned', 'customerspecific', 'etc', 'europeanstyle', \
              'fruitbased', 'greenhousegrown', 'healthconscious', 'healthrelated', 'hempderived', 'hormonefree', 'legumebased', 'licenseeowned', \
              'nutrientrich', 'oilroasted', 'patentpending', 'probioticbased', 'proteinfiber', 'relapsedrefractory', 'scientificallyvalidated', \
              'selling', 'serves', 'statechartered', 'upanddownthestreet', 'vegetablebased', 'visionspecific', 'waterbased', 'weightmanagement'
              ]
    replacement_5 = ['business', 'business', 'company', 'company', 'company', 'company', 'company', 'company', 'company', \
                     'design', 'design', 'develop', 'develop', 'distribute', 'distribute', 'technology', 'technology', \
                     'include', 'include', 'industry', 'industry', 'industry', 'international', 'international', 'international', \
                     'manufacture', 'manufacture', 'manufacture', 'market', 'market', 'operate', 'operate', 'operate', 'own', 'own', \
                     'produce', 'produce', 'product', 'product', 'service', 'service', 'supply', 'supply', 'system', 'system', \
                     # mega
                     'uk', 'us', 'china', 'us', 'agriculture', ' ', 'aquarium based', ' ', ' ', ' ', 'biotech', 'cheese flavored', \
                     'company owned', 'customerspecific', ' ', 'european style', 'fruit based', 'greenhouse grown', 'health conscious', \
                     'health related', 'hemp derived', 'hormone free', 'legume based', 'licensee owned', 'nutrient rich', 'oil roasted', \
                     'patent pending', 'probiotic based', 'protein fiber', 'relapsed refractory', 'scientifically validated', 'selling', \
                     'serves', 'state chartered', 'up and down the street', 'vegetable based', 'vision specific', 'water based', \
                     'weight management']
    raw_20211101 = ['llc','corp']
    replacement_20211101 = ['company','company']

    #chronology from mega's workout
    raw = raw_1 + raw_2 + raw_3 + raw_4 + raw_5 + raw_20211101
    replacement = replacement_1 + replacement_2 + replacement_3 + replacement_4 + replacement_5 + replacement_20211101

    # all LOWERED, sorted, no duplicates(set from OrderedDict and chronology of adding and sorting remains) (sorted list, so as not to remove partially from longer phrase-cases, will go from longer to shorter)
    raw = list(map(str.lower,raw))
    replacement = list(map(str.lower,replacement))

    textclean_phrases_dict_2 = \
        OrderedDict(                                                #sorted order remains
            sorted(filter(lambda x: len(x[0])>0,
                          OrderedDict(zip(raw,replacement)).items()), #chronology from mega's workout remains coz of order of adding remains
                   key=lambda x: (-len(x[0]),x[0]), reverse=False))


    #######################################
    ## textclean_phrases_dict_3:OrderedDict
    ## case-lowered: (replacings to whitespaces) nationalities, continents, countries, us-states, usual english stop-words, special cases words (founded and formulated manually)
    #######################################

    list_prep = []

    #nationalities
    list_prep += list(GeoText("").index.nationalities.keys())

    #continents
    geo_collection = gnc.get_continents()
    #continents - long names
    list_prep += [an_n['name'] for c in geo_collection for an_n in geo_collection[c]['alternateNames'] if an_n['lang']=='en']

    #countries
    geo_collection = gnc.get_countries()
    #countries - long names
    list_prep += [geo_collection[c]['name'] for c in geo_collection]

    #US states
    geo_collection = gnc.get_us_states()
    list_prep += [geo_collection[c]['name'] for c in geo_collection]

    #english stop-words
    stop = stopwords.words('english')

    #general business words
    biz_stop = ['business', 'businesses', 'company', 'companies', 'include', 'includes', 'industry', 'industries',
                'international', 'use', 'uses', 'approximately', 'also', 'based', 'client', 'clients', 'customer',
                'customers', 'name', 'names', 'primarily', 'segment', 'segments', 'sell', 'sells', 'subsidiary',
                'subsidiaries', 'focuses', 'focus', 'formerly', 'incorporated', 'headquartered', 'founded',
                'holdings', 'located', 'engages', 'related', 'together', 'various', 'provide', 'provides',
                "general", "group", "market", "operate", "activities",
                'well', 'consumer', 'united', 'states', 'addition', 'york',
                'working', 'million', 'billion', 'hundred', 'thousand', 'dozen', 'america', 'americas', 'significant', 'offer', 'offers', 'apply','applies']

    month_words_stop = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
    num_words_stop = ["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
                      "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen",
                      "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety",
                      "hundred", "thousand", "million", "billion",
                      "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth",
                      "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth", "sixteenth", "seventeenth", "eighteenth", "nineteenth",
                      "twentieth", "thirtieth", "fortieth", "fiftieth", "sixtieth", "seventieth", "eightieth", "ninetieth",
                      "hundredth", "thousandth", "millionth", "billionth"]
    cardinal_directions_stop = ["wnw","nnw","nne","ene","ese","sse","ssw","wsw","north","east","south","west","northwest","westnorth","northeast","eastnorth","southeast","eastsouth","southwest","westsouth"]
    val_stop = ["small","medium","big"] + ["little","many","any","some"] + ["low","high","average","mean"] + ["start","starts","end","ends","beyond","middle","continue","continues"]

    list_prep += stop + biz_stop + month_words_stop + num_words_stop + cardinal_directions_stop + val_stop

    # all LOWERED, sorted, no duplicates(set) (sorted list, so as not to remove partially from longer phrase-cases, will go from longer to shorter)
    list_prep = sorted(filter(lambda x: len(x)>0,
                              set(map(str.lower,
                                      list_prep))),
                       key=lambda x: (-len(x),x), reverse=False)

    textclean_phrases_dict_3 = OrderedDict.fromkeys(list_prep, " ")

    return (textclean_phrases_dict_1, textclean_phrases_dict_2, textclean_phrases_dict_3)

"""Functions to clean the Description strings"""

def textclean_remove_accent_chars(x: str):
    # we're using here the "regex" module, not just "re" , coz we need "\p{Mn}"-pattern processing
    # unicodedata.normalize('NFKD', x) -> decompose diacriticed characters in pairs of chars: usual char & diacritic char (e.g. accents, umlauts, etc.)
    #\p{Mn} or \p{Non_Spacing_Mark}: a character intended to be combined with another character without taking up extra space (e.g. accents, umlauts, etc.)
    # so regex catches that second separated diacritic char from each pair and sub it to emptiness
    # so we get usual latin chars when possible, instead of that french-like diacriticed stuff:
    # décor -> ['d', 'e', '́', 'c', 'o', 'r'] -> decor
    # yes, such a chars REALLY occuring IN REAL descriptions and we transform them. real example: øçíóÀÇôï×áöè°ãú®éßñüäà -> øcioACoi×aoe°au®eßnuaa
    # and just test this: "Málaga François Phút Hơn 中文" -> "Malaga, Francois Phut Hon 中文"
    # (btw, list(map(str.isalnum, '中文ß'))->[True,True,True] so we left unknown language alone in most cases as-is and that's right (ß is German))
    # maybe we will need to use some translator for non-english descriptions in the future.. (1st:translate, 2nd:use this func as a control shot)
    #NFKD:
    # K = Compatibility : "№"->"No", "™"->"TM", ... but © ® ... so NO "K", just "NFD" (that trademarks and №°× gives False to str.isalnum() so we will clear them in next step)
    # D = Decomposition : "é"->['e', '́'] we want this one
    return regex.sub(r'\p{Mn}', '', unicodedata.normalize('NFD', x)) # NFD, not NFKD


def textclean_all(texts:list,
                  textclean_phrases_dict_1:OrderedDict,           #case-sensitive: (replacings to whitespaces) continents,countries,us-states-long,us-states-iso,us-states-iso3,cities, special cases phrases/words (founded and formulated manually)
                  textclean_phrases_dict_2:OrderedDict,           #case-lowered:   (replacings to phrases)     special cases phrases/words (founded and formulated manually)
                  textclean_phrases_dict_3:OrderedDict):          #case-lowered:   (replacings to whitespaces) nationalities, continents, countries, us-states, usual english stop-words, special cases words (founded and formulated manually)
    """The function to correctly clean out all the dirt (gold-diger for tokens that are relevant to products&services of company)"""

    #case-sensitive phrases removals with Aho-Corasick algo
    #(replaces phrases/words to ws (removings))
    ahc_rootnode = textclean_ahocorasick_createnode(textclean_phrases_dict_1)
    texts = list(map(lambda x: textclean_ahocorasick_processtext(x, ahc_rootnode), texts))

    #convert letters with diacritics to base latin letters, to get more usable tokens further (décor -> decor)
    texts = list(map(lambda x: textclean_remove_accent_chars(x), texts))

    #lower all (no case sensitivene steps further)
    texts = list(map(str.lower, texts))

    #case-insensitive phrases replacings with Aho-Corasick algo
    #(maps phrases/words to other phrases/words or ws if removing)
    ahc_rootnode = textclean_ahocorasick_createnode(textclean_phrases_dict_2)
    texts = list(map(lambda x: textclean_ahocorasick_processtext(x, ahc_rootnode), texts))

    #case-insensitive phrases removals with Aho-Corasick algo
    #(replaces phrases/words to ws (removings))
    ahc_rootnode = textclean_ahocorasick_createnode(textclean_phrases_dict_3)
    texts = list(map(lambda x: textclean_ahocorasick_processtext(x, ahc_rootnode), texts))

    #replace to ws any punctuation char if lookbehind AND lookahead gives 2+ word chars
    rp = re.escape(string.punctuation)
    texts = list(map(lambda x: re.sub("(?<=\w\w)["+rp+"](?=\w\w)", " ", x), texts))
    #remove any left punctuation char if lookbehind AND lookahead gives less than 2+ word chars
    texts = list(map(lambda x: re.sub("(?<=\w)["+rp+"](?=\w)", "", x), texts))
    #replace to ws any left punctuation char
    texts = list(map(lambda x: re.sub("["+rp+"]", " ", x), texts))

    ######################
    ## for now (20211103) we are working with basic latin-letter-words (ascii-letters + some usual chars)
    ## maybe we will expand to some chineese, japaneese, or german with a help of some lang.translator python module to get latin-like-words in any company from any exchange with their local regulatory standarts
    ## but, for now, we did all we could to get meaningfull latin-like words and we must drop off all non "a-Z0-9 {punctuation}" characters (if any)
    # (either trademarks, spec.symbols, etc, which is good to drop, we do have such chars in the texts)
    #replace with ws
    rp = re.escape(string.punctuation + string.ascii_letters + string.digits + " ")
    texts = list(map(lambda x: re.sub("[^"+rp+"]", " ", x), texts))
    ######################

    #remove all 1-symbol words, splitted by ws
    texts = list(map(lambda text: " ".join(filter(lambda x: len(x)>1,
                                                  text.split(sep=" ",maxsplit=-1))),
                     texts))

    #remove all isdigit()-words, splitted by ws
    texts = list(map(lambda text: " ".join(filter(lambda x: not(x.isdigit()),
                                                  text.split(sep=" ",maxsplit=-1))),
                     texts))

    #remove trailing,tailig,doubled whitespaces
    texts = list(map(lambda text: " ".join(filter(lambda x: x!="",
                                                  text.split(sep=" ",maxsplit=-1))),
                     texts))

    #here we have usable words, separated by whitespaces
    #last one step is - steming
    stemmer = SnowballStemmer("english")
    texts = list(map(lambda text: " ".join(map(stemmer.stem,
                                               filter(lambda x: x!="",
                                                      text.split(" ",maxsplit=-1)))),
                     texts))
    return list(texts)


def generate_industrytokenstfidf_vocabs(
        txts_ind:list, #industries
        txts_des:list  #tickers descriptions
)->(
        dict,  #vocab_all_tokens_idf
        dict): #vocab_industry_tokens_tfidfnorm

    #all txts_des suppose to be correctly preprocessed: words separated by whitespace

    #d_all struct:
    #vocab_all_tokens_idf{'token':idf, ...}
    #d_ind struct:
    #vocab_industry_tokens_tfidfnorm{'ind':{'token':tfidfnorm,
    #                                       ...},
    #                                ...}
    #l_tictf struct:
    #[{'token':tf, ...},
    # ...]

    #we're using safe+soft idf(t)=1+log((1+n)/(1+df(t)))
    #where "n" is the total number of industries, and df(t) is the number of industries set that contain term "t".

    vocab_industry_tokens_cnts = dict()
    for ind,des in zip(txts_ind,txts_des):
        vocab_industry_tokens_cnts.setdefault(ind,Counter()).update([t for t in filter(lambda x: x!="", des.split(" ",maxsplit=-1))])

    ind_cnt = Counter(txts_ind)
    vocab_industry_tokens_tfidfnorm = dict()
    vocab_all_tokens_idf = Counter()
    for k,v in vocab_industry_tokens_cnts.items():
        vocab_industry_tokens_tfidfnorm[k] = dict(zip(v.keys(), map(lambda x: x/ind_cnt[k], v.values())))
        vocab_all_tokens_idf.update(v.keys())
    vocab_industry_tokens_cnts = None
    vocab_all_tokens_idf = dict(vocab_all_tokens_idf)
    for k,v in vocab_all_tokens_idf.items():
        vocab_all_tokens_idf[k] = 1+math.log((1+len(ind_cnt))/(1+v))
    for k_i,v_i in vocab_industry_tokens_tfidfnorm.items():
        for k_t,v_t in v_i.items():
            vocab_industry_tokens_tfidfnorm[k_i][k_t] = v_t * vocab_all_tokens_idf[k_t] #tf * idf
        l2norm = sum(map(lambda x: x**2, vocab_industry_tokens_tfidfnorm[k_i].values()))**0.5
        for k_t,v_t in v_i.items():
            vocab_industry_tokens_tfidfnorm[k_i][k_t] /= (1e-30 + l2norm)
    #...better to use the numpy than this...
    return (vocab_all_tokens_idf, vocab_industry_tokens_tfidfnorm)


def tokenize_gettf(texts:list)->list: #returns list of TF dicts (order of list preserved)
    #all texts suppose to be correctly preprocessed: words separated by whitespace
    return list(map(lambda text: dict(Counter([t for t in filter(lambda x: x!="",
                                                                 text.split(" ",maxsplit=-1))])),
                    texts))

