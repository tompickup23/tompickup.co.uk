import { createHash } from 'node:crypto';

/* Fingerprint of an article's text, recorded in its `audio.textHash` when the
   narration is made (scripts/audio/narrate.py computes the same thing). If the
   article is edited afterwards the hashes differ and the player is withheld,
   so the audio never disagrees with the text it stands in for. */
export function textHash(body: string): string {
  return createHash('sha256').update(body.replace(/\s+/g, ' ').trim()).digest('hex');
}

/* "3 min 6 s" for people, "PT3M6S" for schema.org. */
export function spokenDuration(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return m ? `${m} min${s ? ` ${s} s` : ''}` : `${s} s`;
}

export function isoDuration(seconds: number): string {
  return `PT${Math.floor(seconds / 60)}M${seconds % 60}S`;
}
