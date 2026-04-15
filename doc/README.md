# ns_server Documentation

## Viewing Diagrams

Several documents use [Mermaid](https://mermaid.js.org/) for architecture diagrams and sequence flows. VS Code generally offers a more polished visual rendering of Mermaid diagrams.

### Visual Studio Code

1. **Install Support:**
    * Press `Cmd+Shift+X` to open Extensions.
    * Search for [Markdown Preview Mermaid Support](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid) and click **Install**.
2. **Render Preview:**
    * Open any `.md` file.
    * Press `Shift+Cmd+V` to open the rendered preview.
    * *Tip: Click the "Open Preview to the Side" icon in the top-right corner for a split view.*

### IntelliJ IDEA

1. **Install Plugin:**
    * Press `Cmd+,` to open Settings.
    * Go to **Plugins**, search the Marketplace for **Mermaid**, and click **Install**.
2. **Fix File Association** (If preview doesn't appear automatically):
    * Go to **Settings (`Cmd+,`) > Editor > File Types**.
    * Select **Markdown** from the list and add `*.md` to the **File name patterns**.
3. **Render Preview:**
    * Open any `.md` file.
    * Click the **Markdown Split Editor** tab at the **bottom left** of the editor window to see the source and preview side-by-side.
    * *Tip: If the preview pane is blank, click the "Editor and Preview" icon in the top-right corner for a split view.*
