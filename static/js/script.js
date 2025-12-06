// script.js
console.log("📚 Maktabat al-Jamea client loaded");

document.addEventListener("DOMContentLoaded", function () {
  // =======================
  // Toast Notification Helper
  // =======================
  window.showToast = function (message, type = "info") {
    const container = document.getElementById("toastContainer");
    if (!container) return;

    const colorMap = {
      success: "bg-success text-white",
      danger: "bg-danger text-white",
      warning: "bg-warning text-dark",
      info: "bg-info text-dark"
    };

    const toast = document.createElement("div");
    toast.className =
      "toast align-items-center " +
      (colorMap[type] || "bg-info text-dark") +
      " border-0";
    toast.role = "alert";
    toast.innerHTML = `
      <div class="d-flex">
        <div class="toast-body">${message}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto"
                data-bs-dismiss="toast"></button>
      </div>`;

    container.appendChild(toast);

    const bsToast = new bootstrap.Toast(toast, { delay: 3000 });
    bsToast.show();
    toast.addEventListener("hidden.bs.toast", () => toast.remove());
  };

  // =======================
  // Page Elements & Flags
  // =======================
  const reportType      = document.getElementById("reportType");
  const classField      = document.getElementById("classField");
  const deptField       = document.getElementById("deptField");
  const individualField = document.getElementById("individualField");
  const identifierInput = document.getElementById("identifier");
  const form            = document.getElementById("reportForm");
  const output          = document.getElementById("reportOutput");

  // Detect the Reports page (export buttons/header exist there)
  const onReportsPage = !!document.getElementById("exportButtons");

  // =======================
  // Report Form Field Toggling
  // =======================
  function toggleFields() {
    if (!reportType) return;
    const t = reportType.value;

    if (classField)      classField.classList.toggle("d-none", t !== "class_wise");
    if (deptField)       deptField.classList.toggle("d-none", t !== "department_wise");
    if (individualField) individualField.classList.toggle("d-none", t !== "individual");

    if (identifierInput) identifierInput.required = t === "individual";
  }

  if (reportType) {
    reportType.addEventListener("change", toggleFields);
    toggleFields();
  }

  // Helper: remove any DataTables button toolbars inside report output
  function removeInnerExportButtons(scopeEl) {
    (scopeEl || document)
      .querySelectorAll(".dt-buttons, .buttons-excel, .buttons-pdf")
      .forEach((el) => el.remove());
  }

  // =======================
  // Global Submit Handler (disabled on Reports page)
  // =======================
  if (form && !onReportsPage) {
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      const data = new FormData(form);
      if (output) {
        output.innerHTML =
          "<p class='text-info'>Generating report, please wait...</p>";
      }

      // Prefer the blueprint-prefixed endpoint; fall back if needed.
      const endpoints = ["/reports/api/generate_report", "/api/generate_report"];

      (async () => {
        for (const url of endpoints) {
          try {
            const r = await fetch(url, { method: "POST", body: data });
            if (!r.ok) continue;
            const j = await r.json();
            if (j.success && j.html) {
              if (output) {
                // Render HTML
                output.innerHTML = j.html;

                // Turn report tables into DataTables WITHOUT built-in buttons
                window.initDataTables?.(output, /* withButtons */ false);

                // Strip any accidental DT toolbars
                removeInnerExportButtons(output);
              }

              showToast("Report generated successfully", "success");
              return;
            }
          } catch (err) {
            // try next endpoint
          }
        }

        if (output) {
          output.innerHTML =
            "<p class='text-danger'>Error requesting report. Please try again.</p>";
        }
        showToast("Error generating report", "danger");
      })();
    });
  }

  // =======================
  // DataTables Helper (global)
  // =======================
  window.initDataTables = function (scopeEl, withButtons = false) {
    const scope = scopeEl || document;
    const tables = scope.querySelectorAll("table.dataframe, table.dt-enable");

    tables.forEach((table) => {
      const headerRow = table.querySelector("thead tr:last-child");
      const headerCols = headerRow ? headerRow.children.length : 0;
      const bodyRows = Array.from(table.querySelectorAll("tbody tr"));

      if (!headerCols || bodyRows.length === 0) return;
      const mismatch = bodyRows.some(
        (tr) => tr.children.length !== headerCols
      );
      if (mismatch) return;

      if (window.$ && $.fn && $.fn.DataTable) {
        if ($.fn.DataTable.isDataTable(table)) {
          $(table).DataTable().destroy();
        }
        $(table).DataTable({
          paging: false,
          searching: false,
          info: false,
          scrollX: true,
          autoWidth: false,
          dom: withButtons ? "Bfrtip" : "frtip",
          buttons: withButtons
            ? [
                {
                  extend: "excelHtml5",
                  text: "⬇️ Excel",
                  title: document.title
                },
                {
                  extend: "pdfHtml5",
                  text: "⬇️ PDF",
                  title: document.title,
                  orientation: "portrait",
                  pageSize: "A4"
                }
              ]
            : []
        });
      }
    });
  };

  // If the page already has tables on load:
  // - On the Reports page, do NOT show DT buttons.
  // - On other pages (e.g., admin lists), allow buttons by default.
  window.initDataTables(document, /* withButtons */ !onReportsPage);

  // =======================
  // Chart.js Defaults
  // =======================
  if (window.Chart) {
    Chart.defaults.color = "#333";
  }

  window.chartColors = [
    "#3366CC", "#DC3912", "#FF9900", "#109618", "#990099",
    "#0099C6", "#DD4477", "#66AA00", "#B82E2E", "#316395",
    "#994499", "#22AA99", "#AAAA11", "#6633CC", "#E67300",
    "#8B0707", "#329262", "#5574A6", "#3B3EAC"
  ];
});

// =======================
// Resize Charts Responsively (Chart.js v3/v4 safe-ish)
// =======================
window.addEventListener("resize", function () {
  if (!window.Chart) return;

  // Chart.js v3/v4: instances is a Map-like object
  const instances = Chart.instances || {};
  if (typeof instances.forEach === "function") {
    instances.forEach((chart) => chart.resize());
  } else {
    // fallback for older versions
    for (const key in instances) {
      if (Object.prototype.hasOwnProperty.call(instances, key)) {
        const chart = instances[key];
        if (chart && typeof chart.resize === "function") {
          chart.resize();
        }
      }
    }
  }
});
