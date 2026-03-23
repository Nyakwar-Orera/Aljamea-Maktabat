// Professional Admin JavaScript - Consolidated Settings & Program Management
class AdminManager {
  constructor() {
    this.allotments = [];
    this.programs = [];
    this.init();
  }

  init() {
    this.bindEvents();
    this.loadInitialData();
  }

  bindEvents() {
    // User Form
    document.getElementById('userForm')?.addEventListener('submit', (e) => this.handleUserSubmit(e));

    // Management Forms
    document.getElementById('addProgramForm')?.addEventListener('submit', (e) => this.handleProgramSubmit(e));
    document.getElementById('saveEditMarksBtn')?.addEventListener('click', () => this.saveEditedMarks());
    document.getElementById('runEmailNowBtn')?.addEventListener('click', () => this.runEmailReports());
    document.getElementById('runAiNudgeBtn')?.addEventListener('click', () => this.runAiNudge());
    document.getElementById('uploadMarksForm')?.addEventListener('submit', async (e) => {
      e.preventDefault();
      const formData = new FormData(e.target);
      const btn = e.target.querySelector('button[type="submit"]');
      const originalText = btn.innerHTML;
      
      try {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Processing...';
        
        const res = await this.apiPost('/api/upload_program_marks', formData);
        this.showToast(res.message, 'success');
        bootstrap.Modal.getInstance(document.getElementById('uploadMarksModal'))?.hide();
        e.target.reset();
        
        // Refresh everything
        await this.loadInitialData();
        await this.loadTaqeemOverview(); 
      } catch (err) {
        this.showToast(err.message, 'danger');
      } finally {
        btn.disabled = false;
        btn.innerHTML = originalText;
      }
    });

    // Role toggle for User Form
    const roleSelect = document.getElementById('role');
    if (roleSelect) roleSelect.addEventListener('change', (e) => this.toggleRoleFields(e));

    // Tab synchronization - load data when taqeem tab is shown
    const taqeemTab = document.querySelector('button[data-bs-target="#taqeem"]');
    if (taqeemTab) {
      taqeemTab.addEventListener('shown.bs.tab', () => {
        this.loadAllotments();
      });
    }

    // Global Refresh
    window.addEventListener('focus', () => {
      if (document.visibilityState === 'visible') this.loadInitialData();
    });
  }

  async loadInitialData() {
    console.log('🔄 Syncing admin dashboard data...');
    try {
      await Promise.all([
        this.loadUsers(),
        this.loadPrograms(),
        this.loadAudit()
      ]);
    } catch (err) {
      console.error('Initial load failed:', err);
    }
  }

  // ========== USER MANAGEMENT ==========
  async loadUsers() {
    try {
      const data = await this.apiGet('/api/list_users');
      this.populateTable('usersTable', data, {
        username: { label: 'User', sublabel: 'name' },
        email: { label: 'Email' },
        role: { label: 'Role', badge: true },
        campus_branch: { label: 'Branch' },
        class_name: { label: 'Status', calc: row => row.class_name || 'System' }
      }, true);
      const badge = document.getElementById('userCountBadge');
      if (badge) badge.textContent = data.length;
    } catch (err) {
      console.error('User load failed:', err);
    }
  }

  async handleUserSubmit(e) {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    const isUpdate = form.querySelector('button').textContent.includes('Update');
    const endpoint = isUpdate ? '/api/update_user' : '/api/add_user';

    try {
      const res = await this.apiPost(endpoint, formData);
      this.showToast(res.message, 'success');
      form.reset();
      this.resetUserForm();
      this.loadUsers();
    } catch (err) {
      this.showToast(err.message, 'danger');
    }
  }

  resetUserForm() {
    const form = document.getElementById('userForm');
    if (!form) return;
    form.querySelector('[name="username"]').readOnly = false;
    const btn = form.querySelector('button');
    btn.textContent = '💾 Save User Profile';
    btn.className = 'btn btn-primary w-100 py-2';
    this.toggleRoleFields({ target: { value: '' } });
  }

  toggleRoleFields(e) {
    const role = e.target.value;
    const marhala = document.getElementById('marhalaField');
    const darajah = document.getElementById('darajahField');
    const teacher = document.getElementById('teacherName');
    
    [marhala, darajah, teacher].forEach(el => {
      if(el) { el.classList.add('d-none'); el.required = false; }
    });

    if (role === 'hod' && marhala) {
      marhala.classList.remove('d-none');
      marhala.required = true;
    } else if (role === 'teacher') {
      if(darajah) { darajah.classList.remove('d-none'); darajah.required = true; }
      if(teacher) { teacher.classList.remove('d-none'); teacher.required = true; }
    } else if (role === 'student' && darajah) {
      darajah.classList.remove('d-none');
      darajah.required = true;
    }
  }

  async editUser(username) {
    try {
      const users = await this.apiGet('/api/list_users');
      const user = users.find(u => u.username === username);
      if (!user) return;

      const form = document.getElementById('userForm');
      if (!form) return;

      form.querySelector('[name="username"]').value = user.username;
      form.querySelector('[name="username"]').readOnly = true;
      form.querySelector('[name="email"]').value = user.email || '';
      form.querySelector('[name="role"]').value = user.role || '';
      form.querySelector('[name="campus_branch"]').value = user.campus_branch || 'Global';
      
      this.toggleRoleFields({ target: { value: user.role } });
      
      if (user.role === 'hod') document.getElementById('marhalaField').value = user.class_name || '';
      if (user.role === 'teacher') {
        document.getElementById('darajahField').value = user.class_name || '';
        document.getElementById('teacherName').value = user.teacher_name || '';
      }
      if (user.role === 'student') document.getElementById('darajahField').value = user.class_name || '';

      const btn = form.querySelector('button');
      btn.textContent = '💾 Update User Profile';
      btn.className = 'btn btn-warning w-100 py-2';
      
      form.scrollIntoView({ behavior: 'smooth' });
    } catch (err) {
      this.showToast(err.message, 'danger');
    }
  }

  async removeTableRow(tableId, id) {
    if (tableId === 'usersTable') {
      if (!confirm(`Delete user ${id}?`)) return;
      try {
        await this.apiPost('/api/remove_user', { username: id });
        this.showToast('User deleted', 'success');
        this.loadUsers();
      } catch (err) {
        this.showToast(err.message, 'danger');
      }
    }
  }

  // ========== TAQEEM & PROGRAM ENGINE ==========
  async loadPrograms() {
    try {
      const data = await this.apiGet('/api/list_programs');
      // System programs (prepended visually)
      const systemPrograms = [
        {
          id: 'books_issued',
          title: '📚 Books Issued (Automatic List)',
          date: 'Ongoing',
          frequency: 'annually',
          marks: 40,
          marks_category: 'Automatic',
          marhalas: '["All"]',
          venue: 'Koha System',
          conductor: 'System',
          department_note: 'Automatically synced. Do not delete.'
        }
      ];
      
      this.programs = [...systemPrograms, ...(data || [])];
      this.renderProgramsTable();
    } catch (err) {
      console.error('Programs load failed:', err);
    }
  }

  renderProgramsTable() {
    const tbody = document.getElementById("programsTableBody");
    if (!tbody) return;
    
    tbody.innerHTML = "";
    let total = 0;

    if (!this.programs.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="text-center py-5 text-muted"><i class="bi bi-calendar-x fs-3 d-block mb-2"></i>No programs yet. Click "Add New Program" to start.</td></tr>';
    } else {
      const today = new Date().toISOString().split("T")[0];
      
      // Sort: Books Issued FIRST, then date DESC
      const sorted = [...this.programs].sort((a, b) => {
        if (a.title.toLowerCase().includes('books issued')) return -1;
        if (b.title.toLowerCase().includes('books issued')) return 1;
        return new Date(b.date) - new Date(a.date);
      });

      sorted.forEach((prog, idx) => {
        total += parseFloat(prog.marks) || 0;
        let marhalas = [];
        try { 
            const p = JSON.parse(prog.marhalas);
            marhalas = Array.isArray(p) ? p : (p ? [p] : ["All"]);
        } catch { 
            marhalas = prog.marhalas && prog.marhalas !== 'null' ? [prog.marhalas] : ["All"]; 
        }

        const isPast = prog.date && prog.date !== 'Ongoing' && prog.date < today;
        const statusHtml = isPast
          ? '<span class="prog-status-past"><i class="bi bi-lock-fill me-1"></i>Past</span>'
          : '<span class="prog-status-future"><i class="bi bi-arrow-right-circle me-1"></i>' + (prog.date === 'Ongoing' ? 'Permanent' : 'Upcoming') + '</span>';

        const marhalaPills = Array.isArray(marhalas) ? marhalas.map(m => `<span class="marhala-pill">${m}</span>`).join('') : '<span class="marhala-pill">All</span>';
        const categoryColors = { 'Manual': 'secondary', 'Automatic': 'primary', 'Attendance': 'info' };
        const catColor = categoryColors[prog.marks_category] || 'secondary';

        const isSystem = typeof prog.id === 'string' && prog.id === 'books_issued';

        let editBtn = '';
        if (isSystem) {
          editBtn = `<button class="btn btn-sm btn-outline-secondary" disabled title="System program – automatically syncs"><i class="bi bi-gear-fill"></i></button>`;
        } else if (!isPast) {
          editBtn = `<button class="btn btn-sm btn-outline-warning" onclick="adminManager.openEditMarksModal(${prog.id}, '${prog.title.replace(/'/g, "\\'")}', ${prog.marks}, ${isPast})" title="Edit Marks">
               <i class="bi bi-pencil"></i>
             </button>`;
        } else {
          editBtn = `<button class="btn btn-sm btn-outline-secondary" disabled title="Past program – marks locked">
               <i class="bi bi-lock"></i>
             </button>`;
        }

        const uploadBtn = (prog.marks_category !== 'Automatic' && !isSystem)
          ? `<button class="btn btn-sm btn-outline-success" onclick="adminManager.openUploadMarksModal(${prog.id}, '${prog.title.replace(/'/g, "\\'")}')" title="Upload Marks">
               <i class="bi bi-upload"></i>
             </button>`
          : '';
          
        const deleteBtn = !isSystem
          ? `<button class="btn btn-sm btn-outline-danger" onclick="adminManager.deleteProgram(${prog.id}, '${prog.title.replace(/'/g, "\\'")}')" title="Delete Program">
               <i class="bi bi-trash"></i>
             </button>`
          : '';

        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td class="ps-4 text-muted fw-semibold">#${idx + 1}</td>
          <td>
            <div class="fw-bold">${prog.title}</div>
            <div class="small text-muted">${prog.venue || ''}${prog.conductor ? ' · ' + prog.conductor : ''}</div>
            ${prog.department_note ? `<div class="small text-info fst-italic">${prog.department_note}</div>` : ''}
          </td>
          <td>
            <div class="fw-semibold">${prog.date}</div>
            <small class="text-muted text-capitalize">${prog.frequency}</small>
          </td>
          <td>
            <span class="marks-pill">${prog.marks}%</span>
          </td>
          <td>
            <span class="badge bg-${catColor}-subtle border border-${catColor} text-${catColor} text-capitalize">${prog.marks_category}</span>
          </td>
          <td>${marhalaPills}</td>
          <td>${statusHtml}</td>
          <td class="text-end pe-4">
            <div class="btn-group btn-group-sm">
              ${editBtn}
              ${uploadBtn}
              ${deleteBtn}
            </div>
          </td>
        `;
        tbody.appendChild(tr);
      });
    }

    const badge = document.getElementById("programCountBadge");
    if (badge) badge.textContent = `${this.programs.length} programs`;
  }

  updateKPIs() {
    const grandTotal = this.programs.reduce((sum, p) => sum + parseFloat(p.marks), 0);
    const remaining = Math.max(0, 100 - grandTotal);

    const elAllotted = document.getElementById("allottedMarks");
    const elAvailable = document.getElementById("availableMarks");
    const elBar = document.getElementById("marksProgressBar");
    const elText = document.getElementById("progressLabel");
    const elWarning = document.getElementById("limitWarning");

    if (elAllotted) elAllotted.textContent = `${grandTotal.toFixed(1)}%`;
    if (elAvailable) elAvailable.textContent = `${remaining.toFixed(1)}%`;
    
    if (elBar) {
      elBar.style.width = `${Math.min(100, grandTotal)}%`;
      elBar.className = `progress-bar ${grandTotal > 99 ? "bg-danger" : grandTotal > 80 ? "bg-warning" : "bg-primary"}`;
    }
    if (elText) elText.textContent = `${grandTotal.toFixed(1)} / 100%`;
    
    if (elWarning) {
        elWarning.style.display = grandTotal >= 100 ? "block" : "none";
    }

    const marksRemainingText = document.getElementById("marksRemainingText");
    if (marksRemainingText) marksRemainingText.textContent = `${remaining.toFixed(1)}%`;

    const addBtn = document.getElementById("addProgramBtn");
    if (addBtn) {
      if (grandTotal >= 100) {
        addBtn.disabled = true;
        addBtn.innerHTML = '<i class="bi bi-slash-circle me-2"></i>Pool Full';
      } else {
        addBtn.disabled = false;
        addBtn.innerHTML = '<i class="bi bi-plus-circle me-2"></i>Add New Program';
      }
    }
  }



  async handleProgramSubmit(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    try {
      await this.apiPost('/api/add_program', formData);
      this.showToast('Program registered successfully', 'success');
      bootstrap.Modal.getInstance(document.getElementById('addProgramModal')).hide();
      e.target.reset();
      this.loadAllotments();
    } catch (err) {
      this.showToast(err.message, 'danger');
    }
  }

  async deleteProgram(id, title) {
    if (!confirm(`Remove program '${title}'? This will release the mark allotment and clear associated attendance.`)) return;
    try {
      await this.apiPost('/api/remove_program', { id });
      this.showToast('Program removed', 'success');
      this.loadAllotments();
    } catch (err) {
      this.showToast(err.message, 'danger');
    }
  }

  openEditMarksModal(id, title, currentMarks, isPast) {
    document.getElementById("editProgramId").value = id;
    document.getElementById("editProgramTitle").textContent = title;
    document.getElementById("editMarksInput").value = currentMarks;
    document.getElementById("currentMarksDisplay").textContent = currentMarks + "%";
    
    const grandTotal = this.programs.reduce((sum, p) => sum + parseFloat(p.marks), 0);
    const available = 100 - grandTotal + currentMarks; 
    
    document.getElementById("editPoolRemaining").textContent = available.toFixed(1) + "%";
    document.getElementById("editMarksInput").max = available;

    const alert = document.getElementById("pastProgramAlert");
    if (isPast) {
      if(alert) alert.classList.remove("d-none");
      document.getElementById("editMarksInput").min = currentMarks;
    } else {
      if(alert) alert.classList.add("d-none");
      document.getElementById("editMarksInput").min = 0.5;
    }
    new bootstrap.Modal(document.getElementById('editMarksModal')).show();
  }

  async handleEditMarksSubmit(e) {
    // This is now handled by saveEditMarksBtn listener in init/bindEvents if we want,
    // but I will keep a method for consistency.
  }

  async saveEditedMarks() {
    const id = document.getElementById("editProgramId").value;
    const marks = parseFloat(document.getElementById("editMarksInput").value);
    if (!id || isNaN(marks) || marks <= 0) {
      this.showToast("Please enter a valid marks value.", "warning");
      return;
    }
    try {
      await this.apiPost('/api/edit_program_marks', { id: parseInt(id), marks: marks });
      this.showToast('Marks updated successfully', 'success');
      bootstrap.Modal.getInstance(document.getElementById('editMarksModal')).hide();
      this.loadAllotments();
    } catch (err) {
      this.showToast(err.message, 'danger');
    }
  }
  
  // ========== AUDIT LOG ==========
  async loadAudit() {
    try {
      const data = await this.apiGet('/api/list_audit');
      const tbody = document.querySelector('#auditTable tbody');
      if (!tbody) return;

      if (data.length === 0) {
        tbody.innerHTML = `<tr><td colspan="4" class="text-center text-muted py-3">No activity recorded yet.</td></tr>`;
        return;
      }

      tbody.innerHTML = '';
      data.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="small text-muted">${row.ts}</td>
          <td><span class="badge bg-secondary">${row.actor}</span></td>
          <td><span class="text-info">${row.action}</span></td>
          <td class="small">${row.details || ''}</td>
        `;
        tbody.appendChild(tr);
      });
    } catch (err) {
      console.error("Audit load failed", err);
    }
  }

  openUploadMarksModal(id, title) {
    document.getElementById("uploadProgramId").value = id;
    document.getElementById("uploadProgramTitle").textContent = title;
    new bootstrap.Modal(document.getElementById("uploadMarksModal")).show();
  }
  
  async loadTaqeemOverview() {
    try {
      this.showToast("Loading Taqeem Overview...");
      const res = await this.apiGet("/api/get_student_taqeem");
      if (res.success) {
        this.populateTable("taqeemOverviewTable", res.data, {
          username: { label: "Student", sublabel: "name" },
          class_name: { label: "Class/Darajah" },
          total_pd_marks: { label: "Books P/D" },
          total_review_marks: { label: "Reviews" },
          total_program_marks: { label: "Programs" },
          grand_total: { label: "Total %", badge: true }
        }, true, 'taqeem'); 
      }
    } catch(e) {
      console.error(e);
      this.showToast("Failed to load overview");
    }
  }

  async recalcAllTaqeem() {
    if (!confirm("This will recalculate marks for ALL students based on current Koha data and uploaded reviews. Proceed?")) return;
    
    const btn = document.querySelector('button[onclick*="recalcAllTaqeem"]');
    const originalText = btn.innerHTML;
    
    try {
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Calculating...';
      
      const res = await this.apiPost('/api/recalc_all_taqeem', {});
      this.showToast(res.message, 'success');
      await this.loadTaqeemOverview();
    } catch (err) {
      this.showToast(err.message, 'danger');
    } finally {
      btn.disabled = false;
      btn.innerHTML = originalText;
    }
  }

  async runEmailReports() {
    window.confirmAction('Are you sure you want to trigger all report emails now?', async () => {
      try {
        this.showToast('🚀 Triggering email reports...', 'info');
        const res = await this.apiPost('/api/run_email_reports_now', {});
        this.showToast(res.message, 'success');
        this.loadAudit();
      } catch (err) {
        this.showToast(err.message, 'danger');
      }
    }, 'Email Reports');
  }

  async runAiNudge() {
    window.confirmAction('Are you sure you want to trigger all AI nudge emails now?', async () => {
      try {
        this.showToast('🤖 Triggering AI nudges...', 'info');
        const res = await this.apiPost('/api/run_ai_nudge_now', {});
        this.showToast(res.message, 'success');
        this.loadAudit();
      } catch (err) {
        this.showToast(err.message, 'danger');
      }
    }, 'AI Nudge System');
  }

  async exportTaqeemCSV() {
    window.location.href = "/api/export_taqeem";
  }

  // ========== UTILS & TABLE BUILDER ==========
  async apiGet(endpoint) {
    const response = await window.csrfFetch(endpoint);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  }

  async apiPost(endpoint, data) {
    const isFormData = data instanceof FormData;
    const body = isFormData ? data : JSON.stringify(data);
    const headers = isFormData ? {} : { 'Content-Type': 'application/json' };

    const opts = { method: 'POST', body, headers };
    const response = await window.csrfFetch(endpoint, opts);
    const result = await response.json();
    if (!result.success) throw new Error(result.error || 'Operation failed');
    return result;
  }

  populateTable(tableId, data, columns, withActions = false, actionType = 'user') {
    const tbody = document.querySelector(`#${tableId} tbody`);
    if (!tbody) return;
    
    if (!data || data.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${Object.keys(columns).length + (withActions ? 1 : 0)}" class="text-center py-4 text-muted">No records found</td></tr>`;
      return;
    }

    const rows = data.map(row => {
      let tr = '<tr>';
      Object.entries(columns).forEach(([key, conf]) => {
        let val = conf.calc ? conf.calc(row) : (row[key] || '0');
        if (conf.badge) {
          const badgeClass = this.getBadgeClass(val);
          val = `<span class="badge ${badgeClass}">${val}${key === 'grand_total' ? '%' : ''}</span>`;
        } else if (conf.sublabel) {
          val = `<div class="fw-bold">${val}</div><div class="small text-muted">${row[conf.sublabel] || ''}</div>`;
        }
        tr += `<td>${val}</td>`;
      });
      if (withActions) {
        if (actionType === 'user') {
          tr += `
            <td class="text-end">
              <button class="btn btn-sm btn-link" onclick="adminManager.editUser('${row.username || row.id}')"><i class="bi bi-pencil-square"></i></button>
              <button class="btn btn-sm btn-link text-danger" onclick="adminManager.removeTableRow('${tableId}', '${row.username || row.id}')"><i class="bi bi-trash"></i></button>
            </td>
          `;
        } else if (actionType === 'taqeem') {
          tr += `
            <td class="text-end">
              <a href="/student_profile/${row.username}" class="btn btn-sm btn-outline-primary rounded-pill px-3">
                <i class="bi bi-eye me-1"></i>View
              </a>
            </td>
          `;
        }
      }
      tr += '</tr>';
      return tr;
    });

    tbody.innerHTML = rows.join('');
  }

  getBadgeClass(val) {
    const lower = String(val).toLowerCase();
    if (lower.includes('admin')) return 'bg-danger text-white';
    if (lower.includes('student')) return 'bg-success text-white';
    if (lower.includes('hod')) return 'bg-purple text-white';
    if (lower.includes('teacher')) return 'bg-info text-dark';
    return 'bg-secondary text-white';
  }

  showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const bgClass = type === 'success' ? 'bg-success' : type === 'danger' ? 'bg-danger' : 'bg-info';
    const icon = type === 'success' ? 'check-circle' : type === 'danger' ? 'exclamation-circle' : 'info-circle';

    const toastHTML = `
      <div class="toast align-items-center text-white ${bgClass} border-0 show" role="alert" aria-live="assertive" aria-atomic="true">
        <div class="d-flex p-2">
          <div class="toast-body"><i class="bi bi-${icon} me-2 fs-5"></i><span class="fs-6">${message}</span></div>
          <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
      </div>
    `;
    container.insertAdjacentHTML('beforeend', toastHTML);

    const toastEls = container.querySelectorAll('.toast');
    const newToastEl = toastEls[toastEls.length - 1];
    setTimeout(() => {
      newToastEl.classList.remove('show');
      setTimeout(() => newToastEl.remove(), 300);
    }, 4000);
  }
}

// Global UI Hook
function togglePasswordVisibility(inputId) {
  const input = document.getElementById(inputId);
  if (!input) return;
  input.type = input.type === 'password' ? 'text' : 'password';
}

// Instantiate
const adminManager = new AdminManager();
window.adminManager = adminManager;
