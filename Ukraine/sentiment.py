from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lower,
    udf,
    when,
    year,month,
    substring,
    pandas_udf,
)
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import pandas as pd

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

base_path = "/user/s2551055/NewsData/"

# -------------------------------------- Keywords -----------------------------------------
keywords_dict = {
    "en": ["ukraine", "russia"],
    "es": ["ucrania", "rusia"],
    "ru": ["украина", "россия"],
    "ar": ["أوكرانيا", "روسيا"],
}

sentiment_keywords = {
    "en": {
        "positive": [
            "good", "great", "happy", "success", "win", "excellent", "positive", "joy", 
            "reinforce", "gain", "expand", "strengthen", "prosper", "empower", "advance", 
            "aid", "growth", "achievement", "victory", "flourish", "boost", "thriving", 
            "optimistic", "uplift", "support", "hope", "benefit", "encourage", "thrill", 
            "excel", "love", "abundant", "champion", "optimism", "cheerful", "bright", 
            "inspire", "elevate", "happy-go-lucky", "triumph", "celebrate", "achievement", 
            "uplifting", "reward", "favorable", "cheering", "bliss", "delight", "marvelous", 
            "euphoria", "prosperous", "accomplish", "gratifying", "improve", "empowered", 
            "radiant", "sparkle", "beautiful", "ecstatic", "shining", "refreshing", "heartening", 
            "encouraging", "rejuvenating", "exhilarating", "proud", "positive-energy", "energizing", 
            "satisfying", "bloom", "serene", "refreshing", "blissful","amazing"
        ],
        "negative": [
            "bad", "terrible", "sad", "fail", "lose", "horrible", "negative", "angry", 
            "surrender", "retreat", "decline", "weaken", "collapse", "deteriorate", 
            "block", "shrink", "tyrant", "dictator", "suppress", "fail", "crush", "oppress", 
            "defeat", "tragedy", "ruin", "collapse", "devastation", "hopeless", "pain", "harm", 
            "destruction", "terror", "disaster", "waste", "crying", "attack", "misfortune", 
            "cruel", "violent", "agony", "suffer", "disappointment", "grief", "wreck", 
            "disastrous", "painful", "depressing", "misery", "hurt", "doom", "disillusioned", 
            "nightmare", "repression", "devastated", "abandoned", "loss", "tragedy", 
            "downfall", "frustrating", "injury", "toxic", "shattered", "dread", "fear", "crisis",
            "anger", "rage", "hostility", "outrage", "resentment", "frustration", "wrath", "fury",
            "malice", "bitterness", "irritation", "spite", "vengeance", "dismay", "despair", 
            "shame", "guilt", "revenge", "vindictiveness","hits","jail","collusion","propaganda"
        ],
    },
    "es": {
        "positive": [
            "bueno", "excelente", "feliz", "éxito", "ganar", "positivo", "alegría", 
            "reforzar", "ganar", "expandir", "fortalecer", "prosperar", "empoderar", "avanzar", 
            "ayuda", "crecimiento", "logro", "victoria", "florecer", "aumento", "prosperidad", 
            "optimista", "elevar", "apoyar", "esperanza", "beneficiar", "fomentar", "emocionante", 
            "sobresalir", "amor", "abundante", "campeón", "optimismo", "alegría", "brillante", 
            "inspirar", "elevar", "triunfo", "celebrar", "logro", "estimulante", "recompensa", 
            "favorable", "animar", "dicha", "maravilloso", "euforia", "prosperoso", "cumplir", 
            "gratificante", "mejorar", "empoderado", "radiante", "brillar", "hermoso", "ecstatico", 
            "resplandeciente", "refrescante", "alentador", "revitalizante", "exhilarante"
        ],
        "negative": [
            "malo", "terrible", "triste", "fallar", "perder", "horrible", "negativo", "enojado", 
            "rendirse", "retirarse", "declinar", "debilitar", "colapsar", "deteriorarse", 
            "bloquear", "encoger", "tirano", "dictador", "suprimir", "fracaso", "aplastar", "oprimir", 
            "derrota", "tragedia", "ruina", "colapso", "devastación", "sin esperanza", "dolor", "daño", 
            "destrucción", "terror", "desastre", "desperdicio", "llanto", "ataque", "desdicha", 
            "cruel", "violento", "agonia", "sufrir", "decepción", "tristeza", "destrozar", 
            "desastroso", "doloroso", "depresión", "miseria", "daño", "fatalidad", "desilusión", 
            "pesadilla", "represión", "devastado", "abandonado", "pérdida", "tragedia", "hundimiento", 
            "frustrante", "lesión", "tóxico", "destrozado", "temor", "crisis"
        ],
    },
     "ru": {
        "positive": [
            "успех", "победа", "прогресс", "достижение", "отличие", "новость", "выигрыш", 
            "празднование", "торжество", "позитивный", "развитие", "укрепление", "расширение", 
            "повышение", "поддержка", "сотрудничество", "инновация", "будущее", "перспективы", 
            "процветание", "устойчивость", "инвестирование", "улучшение", "оптимизация", "рейтинг", 
            "экспансия", "эффективность", "здоровье", "счастье", "сильный", "стабильность", 
            "профессионализм", "компетенция", "яркость", "будущие возможности", "выгодный", 
            "награда", "чемпион", "предприниматель", "достижения", "открытие", "вдохновение", 
            "радость", "светлый", "заработок", "удовлетворение", "триумф", "уверенность", 
            "рост", "восторженный", "активность", "улучшенный", "новатор", "вдохновляющий", 
            "премия", "новаторский", "старт", "союз", "партнёрство", "запуск", "восстановление", 
            "завоевание", "стратегия", "рекорд", "успешный", "значительный", "создание", "финансирование", 
            "активировать", "победитель", "конкуренция", "выступление", "реализованный", "план", 
            "инвестор", "вознаграждение", "просветление", "поощрение", "коллектив", "актуальный"
        ],
        "negative": [
            "плохой", "ужасный", "грустный", "провал", "потеря", "негативный", "злой", 
            "сдаться", "отступить", "ослабить", "ухудшение", "упадок", "разрушение", 
            "блокировать", "сжаться", "тиран", "диктатор", "подавлять", "поражение", "разрушить", "угнетать", 
            "катастрофа", "разруха", "безнадежность", "боль", "вред", "террор", "несчастье", "пустая трата", 
            "плач", "атака", "неудача", "жестокий", "насилие", "страдание", "обида", "гнев", 
            "разочарование", "горе", "разрушение", "ужас", "беспокойство", "страшный", "бедствие", 
            "несчастный", "отчаяние", "страшный", "израненный", "дискомфорт", "потери", "травма", "шок"
        ],
    },
    "ar": {
        "positive": [
            "جيد", "عظيم", "سعيد", "نجاح", "فوز", "إيجابي", "فرح", 
            "تعزيز", "كسب", "توسع", "تقوية", "ازدهار", "تمكين", "تقدم", 
            "مساعدة", "نمو", "إنجاز", "نصر", "ازدهار", "تفوق", "رفع", "دعم", 
            "أمل", "فائدة", "تشجيع", "إلهام", "ازدهار", "حب", "وفير", "بطل", 
            "تفاؤل", "سعادة", "مشرق", "إلهام", "رفع", "انتصار", "احتفال", 
            "تحقيق", "ملهم", "مكافأة", "مؤيد", "تشجيع", "سعادة", "مذهل", 
            "نشوة", "مزدهر", "تحقيق", "إرضاء", "تحسين", "مستدام", "مشرق", 
            "متفائل", "مشرق", "نشاط", "مليء بالأمل"
        ],
        "negative": [
            "سيء", "فظيع", "حزين", "فشل", "خسارة", "سلبي", "غاضب", 
            "استسلام", "تراجع", "ضعف", "انهيار", "تدهور", "حجب", 
            "انكماش", "طاغية", "دكتاتور", "قمع", "هزيمة", "تدمير", "قمع", 
            "كارثة", "خراب", "يأس", "ألم", "ضرر", "رعب", "مأساة", "إهدار", 
            "بكاء", "هجوم", "مصير سيء", "قاسي", "عنيف", "ألم", "عذاب", "أذى", 
            "خيبة أمل", "حزن", "تدمير", "رعب", "كارثي", "خوف", "مأساة", 
            "صراع", "خسارة", "صعوبة", "مؤلم", "سقوط"
        ],
    },
}


# -------------------------------------- Functions -----------------------------------------
filter_condition = None
for keywords in keywords_dict.values():
    condition = col("title").rlike(rf"\b{keywords[0]}\b") | col("title").rlike(rf"\b{keywords[1]}\b")
    filter_condition = condition if filter_condition is None else filter_condition | condition


def classify_sentiment(text, lang):
    keywords = broadcast_keywords.value.get(lang, {})
    positive_words = set(keywords.get("positive", []))
    negative_words = set(keywords.get("negative", []))

    words = text.split()
    positive_count = sum(words.count(word) for word in positive_words)
    negative_count = sum(words.count(word) for word in negative_words)

    if positive_count > negative_count:
        return "positive"
    elif positive_count < negative_count:
        return "negative"
    else:
        return "neutral"


classify_sentiment_udf = udf(
    lambda text, lang: classify_sentiment(text, lang), StringType()
)

# ------------------------------------- Filtering --------------------------------------
df = spark.read.parquet("/user/s2551055/NewsData_full/*/*.parquet").filter(
    (col("published_date").isNotNull())
    & (col("published_date") >= "2014-01-01")
    & (col("language").isNotNull())
    & (col("title").isNotNull())
    & (col("language").isin("en", "es", "ru", "ar"))
)

df = df.withColumn("year", year(col("published_date")))
df = df.withColumn("month", month(col("published_date")))
df = df.withColumn("title", lower(col("title")))
df = df.filter(filter_condition)
df = df.repartition(20)

# size = df.count()
# print("Rows after filtering: ", size)

# --------------------------------- Sentiment analysis ----------------------------------

broadcast_keywords = spark.sparkContext.broadcast(sentiment_keywords)

df_with_sentiment = df.withColumn(
    "sentiment", classify_sentiment_udf(col("title"), col("language"))
)

# temp = df_with_sentiment.select("title", "language", "year", "sentiment").limit(500)
# data = temp.collect()
# pan = pd.DataFrame(
#     data, columns=["title", "language", "year", "sentiment"]
# )
# pan.to_csv("test.csv", index=False)


final_result = (
    df_with_sentiment.groupBy("language", "year", "sentiment")
    .count()
    .orderBy("language", "year", "sentiment")
)

#final_result.show(100)
result_data = final_result.collect()
df_pandas = pd.DataFrame(
    result_data, columns=["language", "year", "sentiment", "count"]
)
df_pandas.to_csv("title_full.csv", index=False)
