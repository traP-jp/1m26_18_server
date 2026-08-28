import { deadline } from "@std/async"
import { Player } from "textalive-app-api"
import type {
  Beat,
  FetchedSongData,
  Segment,
} from "../../api/om26_18.schemas.ts"

interface FetchSongDataParams {
  songUrl: string
  token: string
}

const TIMEOUT_MS = 5_000

const fetchSongData = async (
  { songUrl, token }: FetchSongDataParams,
): Promise<FetchedSongData> => {
  const player = new Player({
    app: {
      token,
    },
  })

  type Video = Awaited<ReturnType<typeof player.createFromSongUrl>>

  const video: Video | null = await deadline(
    player.createFromSongUrl(songUrl),
    TIMEOUT_MS,
  )

  if (!video) {
    throw new Error(`song not found: ${songUrl}`)
  }

  const beats = player.data.songMap.beats.map((beat) => ({
    startsAtMs: beat.startTime,
    endsAtMs: beat.endTime,
  } satisfies Beat)).sort((a, b) => a.startsAtMs - b.startsAtMs)
  const rawSegments = player.data.songMap.segments
  const segments = rawSegments.flatMap((rawSegment) => (
    rawSegment.segments.map((segment) => ({
      isChorus: rawSegment.chorus,
      startsAtMs: segment.startTime,
      endsAtMs: segment.endTime,
    } satisfies Segment))
  )).sort((a, b) => a.startsAtMs - b.startsAtMs)

  const isComplete = !!(player.data.song.artist.name && player.data.song.name)

  if (isComplete) {
    const phrases = video.phrases.map((phrase) => ({
      text: phrase.text,
      startsAtMs: phrase.startTime,
      endsAtMs: phrase.endTime,
    })).sort((a, b) => a.startsAtMs - b.startsAtMs)
    return {
      type: "complete",
      artist: player.data.song.artist.name,
      title: player.data.song.name,
      durationMs: video.duration,
      beats,
      phrases,
      segments,
    }
  }

  return {
    type: "incomplete",
    durationMs: video.duration,
    beats,
    segments,
  }
}

if (import.meta.main) {
  const songUrl = Deno.args.at(0)

  if (!songUrl) {
    console.error("song URL not specified")
    Deno.exit(1)
  }

  const token = Deno.env.get("TEXTALIVE_APP_TOKEN")

  if (!token) {
    console.error("TEXTALIVE_APP_TOKEN not specified")
    Deno.exit(1)
  }

  const tmpFilePath = Deno.args.at(1)

  if (!tmpFilePath) {
    console.error("tmp file path not specified")
    Deno.exit(1)
  }

  try {
    const data = await fetchSongData({ songUrl, token })

    Deno.writeTextFileSync(tmpFilePath, JSON.stringify(data))
  } catch (err) {
    console.error(err)
    Deno.exit(1)
  }
}
