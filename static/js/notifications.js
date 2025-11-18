// static/js/notifications.js
document.addEventListener('DOMContentLoaded', () => {
  const notificationBadge = document.getElementById('notification-badge');
  const notificationDropdown = document.getElementById('notification-dropdown');

  // Fetch notifications
  fetch('/api/notifications')
    .then(response => response.json())
    .then(data => {
      if (data.length > 0) {
        notificationBadge.textContent = data.length;
        notificationDropdown.innerHTML = data.map(n => `
          <li><a class="dropdown-item" href="#">${n.message}</a></li>
        `).join('');
      } else {
        notificationBadge.textContent = '0';
        notificationDropdown.innerHTML = '<li>No notifications</li>';
      }
    });

  // Mark notification as read when clicked
  notificationDropdown.addEventListener('click', (e) => {
    const notificationId = e.target.getAttribute('data-id');
    if (notificationId) {
      fetch('/api/mark_notification_read', {
        method: 'POST',
        body: new URLSearchParams({ id: notificationId }),
      })
      .then(response => response.json())
      .then(data => {
        if (data.success) {
          e.target.classList.add('read');
          notificationBadge.textContent -= 1;
        }
      });
    }
  });
});
