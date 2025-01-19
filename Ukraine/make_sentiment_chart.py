import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("./Ukraine/both_results.csv")  
df = df.pivot_table(index=["language", "year"], 
                     columns="sentiment", 
                     values="count", 
                     fill_value=0).reset_index()

df.columns = ["language", "year", "negative", "neutral","positive"]
df["percent_positive"] = df["positive"] / (df["positive"] + df["negative"] + df["neutral"]) * 100
df["percent_negative"] = df["negative"] / (df["positive"] + df["negative"] + df["neutral"]) * 100

plt.figure(figsize=(12, 6))
for lang in df["language"].unique():
    lang_data = df[df["language"] == lang]
    plt.plot(lang_data["year"], lang_data["percent_positive"], marker='o', label=lang)

plt.xlabel("Year")
plt.ylabel("% Positive Sentiment")
plt.title("Positive Sentiment Percentage Over the Years by Language")
plt.legend()
plt.grid(True)
plt.savefig("./Ukraine/sentiment_chart_positive.png")


plt.figure(figsize=(12, 6))
for lang in df["language"].unique():
    lang_data = df[df["language"] == lang]
    plt.plot(lang_data["year"], lang_data["percent_negative"], marker='o', label=lang)

plt.xlabel("Year")
plt.ylabel("% Negative Sentiment")
plt.title("Negative Sentiment Percentage Over the Years by Language")
plt.legend()
plt.grid(True)
plt.savefig("./Ukraine/sentiment_chart_negative.png")