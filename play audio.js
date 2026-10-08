document.addEventListener('DOMContentLoaded', () => {
  const audio = document.getElementById('hintergrund-musik');
  const musicBtn = document.getElementById('music-btn');

  if (audio && musicBtn) {
    musicBtn.addEventListener('click', () => {
      if (audio.paused) {
        audio.play().then(() => {
          musicBtn.textContent = '⏸️ Musik aus';
        }).catch(err => {
          console.error("Audio konnte nicht abgespielt werden:", err);
        });
      } else {
        audio.pause();
        musicBtn.textContent = '🎵 Musik an';
      }
    });
  }
});
