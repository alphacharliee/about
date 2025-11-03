from event_classifier import classify_event
from news_scraper import get_headlines, extract_full_article_text
from analyzer import analyze_article
from rich import print


if __name__ == "__main__":
    headlines = get_headlines()

    for news in headlines:
        print(f"[bold yellow]{news['source']}[/bold yellow] 🗞️  [dim]{news['published']}[/dim]")
        print(f"[bold cyan]{news['title']}[/bold cyan]")
        print(news['description'])
        print(f"[green]Editorial/Opinion?[/green] {'✅ Yes' if news['editorial'] else '❌ No'}")

        full_text = extract_full_article_text(news['link'])
        if not full_text:
            print(f"[bold red][WARN][/bold red] Could not fetch full article. [link={news['link']}]Read the article here[/link]")
        combined_text = full_text if full_text else f"{news['title']}. {news['description']}"

        event_info = classify_event(combined_text)
        if event_info['event_type']:
            print(f"[bold magenta]Event Type:[/bold magenta] {event_info['event_type']} ({int(event_info['confidence'] * 100)}% confidence)")
        else:
            print("[bold magenta]Event Type:[/bold magenta] Not detected")
        analysis = analyze_article(news['title'], combined_text)
        print(f"[blue]Sentiment:[/blue] {analysis['sentiment_label']} ({analysis['sentiment_score']:.2f})")
        print(f"[magenta]Subjectivity:[/magenta] {analysis['subjectivity']:.2f}")

        if analysis['companies']:
            tickers = ', '.join([f"${c['ticker']}" for c in analysis['companies']])
            print(f"[bold green]Likely affected tickers:[/bold green] {tickers}")
        else:
            print("[bold green]Likely affected tickers:[/bold green] None detected")

        print(f"[link={news['link']}]Read more[/link]\n")