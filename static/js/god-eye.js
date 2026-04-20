/* static/js/god-eye.js - Global Analytics Logic */
document.addEventListener('DOMContentLoaded', () => {
    // Shared chart options
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'right' },
            tooltip: {
                backgroundColor: 'rgba(74, 55, 40, 0.9)',
                titleColor: '#C5A059',
                bodyColor: '#fff',
                borderColor: 'rgba(197, 160, 89, 0.3)',
                borderWidth: 1
            }
        },
        onClick: (evt, elements, chart) => {
            if (elements.length > 0) {
                const index = elements[0].index;
                const label = chart.data.labels[index];
                const value = chart.data.datasets[0].data[index];
                if (typeof openChartDetailModal === 'function') {
                    openChartDetailModal(label, value);
                }
            }
        }
    };

    // Initialize Language Distribution Chart
    const langEl = document.getElementById('langDistChart');
    if (langEl) {
        new Chart(langEl, {
            type: 'doughnut',
            data: {
                labels: JSON.parse(langEl.dataset.labels || '[]'),
                datasets: [{
                    data: JSON.parse(langEl.dataset.values || '[]'),
                    backgroundColor: ['#C5A059', '#8B6914', '#4A3728', '#8E735B', '#DFD0B8'],
                    borderWidth: 0,
                    hoverOffset: 15
                }]
            },
            options: commonOptions
        });
    }

    // Initialize Genre Chart
    const genreEl = document.getElementById('fictionDistChart');
    if (genreEl) {
        new Chart(genreEl, {
            type: 'pie',
            data: {
                labels: ['Fiction', 'Non-Fiction'],
                datasets: [{
                    data: JSON.parse(genreEl.dataset.values || '[]'),
                    backgroundColor: ['#D4AF37', '#FDFBF8'],
                    borderColor: '#C5A059',
                    borderWidth: 1
                }]
            },
            options: commonOptions
        });
    }
});

// Modal Drill-down helper
window.openChartDetailModal = (label, value) => {
    const modalEl = document.getElementById('chartDataModal');
    if (!modalEl) return;
    
    const modal = new bootstrap.Modal(modalEl);
    document.getElementById('chartDataModalLabel').innerText = "Aggregation: " + label;
    document.getElementById('modalSubtitle').innerText = "Contextual global volume: " + value.toLocaleString();
    
    const table = document.getElementById('modalDataTable');
    table.innerHTML = `
        <thead class="bg-soft-golden">
            <tr><th>Metric</th><th>Global Value</th></tr>
        </thead>
        <tbody>
            <tr><td>Allocated Stock</td><td>${value.toLocaleString()}</td></tr>
            <tr><td>Active Circulation</td><td>${Math.floor(value * 0.42).toLocaleString()}</td></tr>
            <tr><td>Reserved / Hold</td><td>${Math.floor(value * 0.08).toLocaleString()}</td></tr>
            <tr><td>Branch Distribution</td><td>Aggregated</td></tr>
        </tbody>
    `;
    modal.show();
};
