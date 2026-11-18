# Carpeta de estudio — PC3

## Contenido

| Archivo | Para qué |
|---|---|
| `Guion_Exposicion_PC3.pdf` | Guion oral completo → súbelo al MLL / NotebookLM |
| `01_prompt_audio.md` | Prompt para ensayar en **audio** / TTS |
| `02_prompt_video.md` | Prompt para ensayar en **video** / storyboard |
| `03_prompt_quiz.md` | Prompt para **quiz** tipo profesor |

El markdown original del guion sigue en: `../Guion_Exposicion_PC3.md`

## Cómo usarlo (orden recomendado)

1. Sube `Guion_Exposicion_PC3.pdf` a la herramienta (NotebookLM, Gemini, ChatGPT, etc.).
2. Copia el bloque entre \`\`\`text ... \`\`\` del prompt 1 (audio) y pégalo.
3. Luego el prompt 2 (video), adjuntando también `../Presentacion_PC3.pdf` si puedes.
4. El mismo día de la expo: prompt 3 (quiz), ronda A → D.

## Nota

`_md_to_pdf.py` regenera el PDF si editas el `.md` del guion:

```powershell
python presentacion\estudio\_md_to_pdf.py
```
