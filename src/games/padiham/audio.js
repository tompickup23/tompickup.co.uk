// All sound is synthesised in the browser, so there are no audio files to load.
export class Sound {
  constructor() { this.ctx = null; this.muted = false; }
  start() {
    if (this.ctx) return;
    const AC = window.AudioContext || window.webkitAudioContext;
    if (!AC) return;
    const ctx = this.ctx = new AC();
    this.master = ctx.createGain(); this.master.gain.value = this.muted ? 0 : 1; this.master.connect(ctx.destination);
    const buf = ctx.createBuffer(1, ctx.sampleRate * 2, ctx.sampleRate), d = buf.getChannelData(0);
    for (let i = 0; i < d.length; i++) d[i] = Math.random() * 2 - 1;
    this.noise = buf;
    // quiet town hum
    const src = ctx.createBufferSource(); src.buffer = buf; src.loop = true;
    const lp = ctx.createBiquadFilter(); lp.type = 'lowpass'; lp.frequency.value = 260;
    const g = ctx.createGain(); g.gain.value = 0.09;
    src.connect(lp).connect(g).connect(this.master); src.start();
  }
  setMuted(m) { this.muted = m; if (this.master) this.master.gain.value = m ? 0 : 1; }
  burst(dur, freq, q, gain, when = 0) {
    if (!this.ctx) return;
    const ctx = this.ctx, t = ctx.currentTime + when;
    const src = ctx.createBufferSource(); src.buffer = this.noise;
    const f = ctx.createBiquadFilter(); f.type = 'bandpass'; f.frequency.value = freq; f.Q.value = q;
    const g = ctx.createGain(); g.gain.setValueAtTime(gain, t); g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    src.connect(f).connect(g).connect(this.master); src.start(t, Math.random(), dur + 0.02);
  }
  tone(freq, dur, gain, when = 0, to = freq, type = 'sine') {
    if (!this.ctx) return;
    const ctx = this.ctx, t = ctx.currentTime + when;
    const o = ctx.createOscillator(); o.type = type; o.frequency.setValueAtTime(freq, t); o.frequency.linearRampToValueAtTime(to, t + dur);
    const g = ctx.createGain(); g.gain.setValueAtTime(0.0001, t); g.gain.linearRampToValueAtTime(gain, t + 0.02); g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    o.connect(g).connect(this.master); o.start(t); o.stop(t + dur + 0.05);
  }
  step(run) { this.burst(0.06, 700 + Math.random() * 500, 1.2, run ? 0.16 : 0.1); }
  flap(n) { for (let i = 0; i < Math.min(14, n * 2); i++) this.burst(0.07, 1400 + Math.random() * 1200, 0.8, 0.12, i * 0.045 + Math.random() * 0.03); this.coo(0.25); }
  coo(when = 0) { this.tone(430, 0.22, 0.05, when, 360); this.tone(390, 0.3, 0.05, when + 0.26, 330); }
  pickup() { this.tone(660, 0.12, 0.12); this.tone(880, 0.16, 0.12, 0.1); }
  done() { [523, 659, 784, 1047].forEach((f, i) => this.tone(f, 0.22, 0.12, i * 0.1, f, 'triangle')); }
}
