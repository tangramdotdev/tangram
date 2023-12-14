;;; tg-mode --- Summary
;;; tg-mode is a major mode for editing Tangram typescript.
;;; Commentary:
;;; Must install the tg binary somewhere on your $PATH.
;; -*- lexical-binding: t; -*-
;;; tg-mode/config.el
;;; Code:

(require 'projectile)
(require 'lsp-mode)
(require 'typescript-mode)

;; The major mode is derived from typescript-mode.
(define-derived-mode tg-mode typescript-mode "Tangram Typescript"
  "Major mode for editing .tg files.")
 
;; Associate tg-mode with .tg files.
;;;###autoload
(add-to-list 'auto-mode-alist '("\\.tg\\'" . tg-mode))

;; Set up lsp-mode.
(with-eval-after-load 'lsp-mode
  (add-to-list 'lsp-language-id-configuration
               '(tg-mode . "Tangram Typescript"))
  (lsp-register-client
   (make-lsp-client :new-connection (lsp-stdio-connection '("tg" "lsp"))
                    :activation-fn (lsp-activate-on "Tangram Typescript")
                    :language-id 'tglsp)))

;; Create package hook to ensure new projects are loaded.
(defun tg-mode-setup ()
  ;; Start the lsp.
  (lsp-deferred))

(add-hook 'tg-mode-hook 'tg-mode-setup)

(provide 'tg-mode)

;;; tg-mode.el ends here
