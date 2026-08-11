import { deadline } from "@std/async"
import { Player } from "textalive-app-api"
import type { SongData } from "../../api/om26_18.schemas.ts"

interface FetchSongDataParams {
  songUrl: string
  token: string
}

const TIMEOUT_MS = 5_000

const fetchSongData = async (
  { songUrl, token }: FetchSongDataParams,
): Promise<SongData> => {
  const player = new Player({
    app: {
      token,
    },
  })

  type Video = Awaited<ReturnType<typeof player.createFromSongUrl>>

  // `createFromSongUrl` returns `null` if the song is not found
  const video: Video | null = await deadline(
    player.createFromSongUrl(songUrl),
    TIMEOUT_MS,
  )

  if (!video) {
    throw new Error(`song not found: ${songUrl}`)
  }

  return player.data.song.artist.name && player.data.song.name
    ? {
      type: "complete",
      artist: player.data.song.artist.name,
      title: player.data.song.name,
      durationMs: video.duration,
    }
    : {
      type: "incomplete",
      durationMs: video.duration,
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

    console.log(data)
    Deno.writeTextFileSync(tmpFilePath, JSON.stringify(data))
  } catch (err) {
    console.error(err)
    Deno.exit(1)
  }
}
