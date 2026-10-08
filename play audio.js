const audio = document.getElementById('hintergrund-musik');
const musicBtn = document.getElementById('music-btn');

musicBtn.addEventListener('click', () => {
  if (audio.paused) {
    audio.play();
    musicBtn.textContent = '⏸️ Musik aus';
  } else {
    audio.pause();
    musicBtn.textContent = '🎵 Musik an';
  }
});
