// Function to handle button clicks
function handleButtonClick(type) {
    // Use Streamlit's setComponentValue to communicate with Python
    if (window.Streamlit) {
        const value = type === 'clear' ? 'clear' : 'save';
        window.Streamlit.setComponentValue(value);
        
        // Show loading state on the button
        const button = document.getElementById(`${type}ChatBtn`);
        if (button) {
            const originalText = button.innerHTML;
            button.innerHTML = type === 'clear' ? 'Clearing...' : 'Saving...';
            button.disabled = true;
            
            // Reset button state after a short delay
            setTimeout(() => {
                button.innerHTML = originalText;
                button.disabled = false;
            }, 1500);
        }
        
        // If it's a save action, show a toast
        if (type === 'save') {
            showToast('Chat saved successfully!', 'success');
        }
    }
    return false;
}

// Function to show toast notifications
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast-notification ${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);
    
    // Add toast styles if not already added
    if (!document.getElementById('toast-styles')) {
        const style = document.createElement('style');
        style.id = 'toast-styles';
        style.textContent = `
            .toast-notification {
                position: fixed;
                bottom: 20px;
                right: 20px;
                padding: 12px 20px;
                border-radius: 4px;
                color: white;
                font-size: 14px;
                z-index: 10000;
                opacity: 0;
                transform: translateY(20px);
                transition: opacity 0.3s, transform 0.3s;
            }
            .toast-notification.show {
                opacity: 1;
                transform: translateY(0);
            }
            .toast-notification.success {
                background-color: #4CAF50;
            }
            .toast-notification.error {
                background-color: #f44336;
            }
        `;
        document.head.appendChild(style);
    }
    
    // Trigger reflow to ensure styles are applied before showing
    void toast.offsetWidth;
    
    // Show toast
    toast.classList.add('show');
    
    // Auto-hide after 3 seconds
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            toast.remove();
        }, 300);
    }, 3000);
}

// Function to initialize chat interface
function initializeChatInterface() {
    // Move Streamlit's chat input to our custom container
    function moveChatInput() {
        const chatInput = document.querySelector('.stChatInputContainer');
        const customInputArea = document.getElementById('customInputArea');
        const streamlitChatInput = document.getElementById('streamlitChatInput');
        
        if (chatInput && customInputArea && streamlitChatInput) {
            // Move the chat input to our custom container
            streamlitChatInput.appendChild(chatInput);
            
            // Show our custom input area
            customInputArea.style.display = 'flex';
            
            // Style the chat input container
            const chatInputContainer = streamlitChatInput.querySelector('.stChatInputContainer');
            if (chatInputContainer) {
                chatInputContainer.style.margin = '0';
                chatInputContainer.style.width = '100%';
                
                // Style the input field
                const inputField = chatInputContainer.querySelector('.stTextInput input');
                if (inputField) {
                    inputField.style.borderRadius = '20px';
                    inputField.style.padding = '10px 15px';
                }
            }
            
            return true;
        }
        return false;
    }
    
    // Set up button event listeners
    function setupButtons() {
        // Clear chat button
        const clearChatBtn = document.getElementById('clearChatBtn');
        if (clearChatBtn) {
            clearChatBtn.onclick = (e) => {
                e.preventDefault();
                return handleButtonClick('clear');
            };
        }
        
        // Save chat button
        const saveChatBtn = document.getElementById('saveChatBtn');
        if (saveChatBtn) {
            saveChatBtn.onclick = (e) => {
                e.preventDefault();
                return handleButtonClick('save');
            };
        }
    }
    
    // Try to move the chat input and set up buttons
    if (moveChatInput()) {
        setupButtons();
    } else {
        // If not found yet, wait and try again
        const checkInterval = setInterval(() => {
            if (moveChatInput()) {
                setupButtons();
                clearInterval(checkInterval);
            }
        }, 100);
    }
    
    // Auto-scroll to bottom of messages
    const messagesContainer = document.querySelector('.messages-container');
    if (messagesContainer) {
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
    }
}

// Initialize when the page loads
document.addEventListener('DOMContentLoaded', initializeChatInterface);

// Also re-initialize after Streamlit updates
if (window.Streamlit) {
    window.Streamlit.events.addEventListener(Streamlit.RENDER_EVENT, initializeChatInterface);
}

// Re-initialize after a short delay to catch any dynamic content
setTimeout(initializeChatInterface, 1000);
