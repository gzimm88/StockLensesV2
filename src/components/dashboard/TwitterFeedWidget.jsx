import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ExternalLink } from "lucide-react";
import { useTheme } from "next-themes";

const FINANCIAL_FEEDS = [
  { handle: "@markets", name: "Bloomberg Markets", url: "https://x.com/markets", desc: "Global markets news" },
  { handle: "@business", name: "Bloomberg", url: "https://x.com/business", desc: "Business & finance" },
  { handle: "@WSJ", name: "Wall Street Journal", url: "https://x.com/WSJ", desc: "Breaking financial news" },
  { handle: "@CNBC", name: "CNBC", url: "https://x.com/CNBC", desc: "Market coverage & analysis" },
  { handle: "@ReutersBiz", name: "Reuters Business", url: "https://x.com/ReutersBiz", desc: "Global business wire" },
  { handle: "@FT", name: "Financial Times", url: "https://x.com/FT", desc: "World business news" },
];

function XLogo({ className }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
    </svg>
  );
}

export default function TwitterFeedWidget() {
  const { resolvedTheme } = useTheme();
  const containerRef = React.useRef(null);
  const [embedLoaded, setEmbedLoaded] = React.useState(false);

  // Attempt to load Twitter embed in background — show fallback links immediately
  React.useEffect(() => {
    let timeoutId;
    const script = document.createElement("script");
    script.src = "https://platform.twitter.com/widgets.js";
    script.async = true;
    script.charset = "utf-8";

    script.onload = () => {
      if (window.twttr?.widgets) {
        window.twttr.widgets.load(containerRef.current);
        timeoutId = setTimeout(() => {
          const container = containerRef.current;
          if (!container) return;
          const iframe = container.querySelector("iframe");
          // Only consider loaded if there's a visible iframe with real content
          if (iframe && iframe.offsetHeight > 100) {
            setEmbedLoaded(true);
          }
        }, 5000);
      }
    };

    document.body.appendChild(script);
    return () => {
      clearTimeout(timeoutId);
      if (script.parentNode) script.parentNode.removeChild(script);
    };
  }, []);

  return (
    <Card className="h-full">
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center gap-2">
          <XLogo className="w-4 h-4" /> Financial Feeds
        </CardTitle>
      </CardHeader>
      <CardContent>
        {/* Twitter embed — hidden until loaded */}
        <div
          ref={containerRef}
          className={embedLoaded ? "" : "hidden"}
          style={{ maxHeight: 400, overflowY: "auto" }}
        >
          <a
            className="twitter-timeline"
            data-height="380"
            data-theme={resolvedTheme === "dark" ? "dark" : "light"}
            data-chrome="noheader nofooter noborders transparent"
            href="https://twitter.com/markets"
          >
            Loading...
          </a>
        </div>

        {/* Fallback links — always visible until embed loads */}
        {!embedLoaded && (
          <div className="space-y-1">
            {FINANCIAL_FEEDS.map((feed) => (
              <a
                key={feed.handle}
                href={feed.url}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between py-2.5 px-2 -mx-2 rounded hover:bg-slate-100 dark:hover:bg-slate-800/50 group transition-colors"
              >
                <div>
                  <p className="text-sm font-medium text-slate-800 dark:text-slate-200">{feed.name}</p>
                  <p className="text-xs text-slate-500 dark:text-slate-400">{feed.desc}</p>
                </div>
                <ExternalLink className="w-3.5 h-3.5 text-slate-400 group-hover:text-slate-600 dark:group-hover:text-slate-300 flex-shrink-0" />
              </a>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
