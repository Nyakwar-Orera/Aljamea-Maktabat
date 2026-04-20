/* static/js/branch-teleport.js - Branch Switching Visuals */
function teleportToBranch(url, branchName) {
    // Create overlay
    const overlay = document.createElement('div');
    overlay.style.position = 'fixed';
    overlay.style.top = '0';
    overlay.style.left = '0';
    overlay.style.width = '100vw';
    overlay.style.height = '100vh';
    overlay.style.backgroundColor = 'var(--color-brown, #4A3728)';
    overlay.style.zIndex = '9999';
    overlay.style.display = 'flex';
    overlay.style.flexDirection = 'column';
    overlay.style.alignItems = 'center';
    overlay.style.justifyContent = 'center';
    overlay.style.color = '#C5A059';
    overlay.style.opacity = '0';
    overlay.style.transition = 'opacity 0.4s ease';
    
    overlay.innerHTML = `
        <div class="teleport-portal" style="width: 120px; height: 120px; border: 4px solid #C5A059; border-radius: 50%; display: flex; align-items: center; justify-content: center; animation: portal-spin 2s linear infinite;">
            <i class="bi bi-eye-fill" style="font-size: 3rem;"></i>
        </div>
        <h2 style="margin-top: 2rem; font-weight: 800; letter-spacing: 2px;">TELEPORTING</h2>
        <p style="text-transform: uppercase; opacity: 0.8; letter-spacing: 1px;">Destination: ${branchName}</p>
        <style>
            @keyframes portal-spin { 0% { transform: rotate(0deg); box-shadow: 0 0 20px #C5A059; } 50% { box-shadow: 0 0 50px #C5A059; } 100% { transform: rotate(360deg); box-shadow: 0 0 20px #C5A059; } }
        </style>
    `;
    
    document.body.appendChild(overlay);
    
    // Trigger fade in
    setTimeout(() => {
        overlay.style.opacity = '1';
    }, 10);
    
    // Redirect after transition
    setTimeout(() => {
        window.location.href = url;
    }, 800);
}

document.addEventListener('DOMContentLoaded', () => {
    // Bind teleport to any campus cards
    document.querySelectorAll('.teleport-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const url = link.getAttribute('href');
            const name = link.dataset.branch || 'Branch Dashboard';
            teleportToBranch(url, name);
        });
    });
});
