document.addEventListener('DOMContentLoaded', function() {
  // Fetch tasks from server and display them
  fetchTasks();

  // Handle form submission for adding new task
  document.getElementById('addTaskForm').addEventListener('submit', function(event) {
    event.preventDefault();
    const formData = new FormData(this);
    const taskData = Object.fromEntries(formData.entries());

    // Validate form data
    if (!taskData.title) {
      alert('Please enter a title for the task.');
      return;
    }

    // Add new task
    addTask(taskData);
    this.reset();
  });
});

// Function to fetch tasks from server
function fetchTasks() {
  fetch('/api/tasks')
    .then(response => response.json())
    .then(tasks => {
      const taskList = document.getElementById('taskList');
      taskList.innerHTML = '';
      tasks.forEach(task => {
        const taskItem = document.createElement('div');
        taskItem.classList.add('task');
        taskItem.innerHTML = `
          <h3>${task.title}</h3>
          <p>${task.description}</p>
          <p>Status: ${task.status}</p>
          <button onclick="deleteTask('${task._id}')">Delete</button>
        `;
        taskList.appendChild(taskItem);
      });
    });
}

// Function to add new task
function addTask(taskData) {
  fetch('/api/tasks', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(taskData)
  })
  .then(response => response.json())
  .then(() => {
    fetchTasks();
  });
}

// Function to delete task
function deleteTask(taskId) {
  fetch(`/api/tasks/${taskId}`, {
    method: 'DELETE'
  })
  .then(() => {
    fetchTasks();
  });
}
