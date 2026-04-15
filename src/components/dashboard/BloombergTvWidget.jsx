import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tv, ChevronDown, ChevronUp, ExternalLink } from "lucide-react";

// Bloomberg Television 24/7 live stream on YouTube.
// Update this ID if the stream URL changes.
const BLOOMBERG_YT_ID = "iEpJwprxDdk";

export default function BloombergTvWidget() {
  const [expanded, setExpanded] = React.useState(false);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center justify-between">
          <span className="flex items-center gap-2">
            <Tv className="w-4 h-4" /> Bloomberg TV
          </span>
          <div className="flex items-center gap-2">
            <a
              href={`https://www.youtube.com/watch?v=${BLOOMBERG_YT_ID}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-300"
            >
              <ExternalLink className="w-3.5 h-3.5" />
            </a>
            <Button
              variant="ghost"
              size="sm"
              className="h-7 px-2 text-xs"
              onClick={() => setExpanded(!expanded)}
            >
              {expanded ? (
                <>Collapse <ChevronUp className="w-3 h-3 ml-1" /></>
              ) : (
                <>Watch Live <ChevronDown className="w-3 h-3 ml-1" /></>
              )}
            </Button>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {expanded ? (
          <div className="aspect-video rounded-lg overflow-hidden bg-black">
            <iframe
              src={`https://www.youtube.com/embed/${BLOOMBERG_YT_ID}?autoplay=0&mute=1`}
              className="w-full h-full"
              loading="lazy"
              allow="accelerometer; encrypted-media; gyroscope; picture-in-picture"
              allowFullScreen
              title="Bloomberg TV Live"
            />
          </div>
        ) : (
          <div
            className="aspect-video rounded-lg bg-slate-100 dark:bg-slate-800 flex flex-col items-center justify-center cursor-pointer hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors"
            onClick={() => setExpanded(true)}
          >
            <Tv className="w-10 h-10 text-slate-400 dark:text-slate-500 mb-2" />
            <p className="text-sm text-slate-500 dark:text-slate-400">Click to watch Bloomberg TV live</p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
