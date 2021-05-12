;; @author Couchbase <info@couchbase.com>
;; @copyright 2015-Present Couchbase, Inc.
;;
;; Use of this software is governed by the Business Source License included
;; in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
;; in that file, in accordance with the Business Source License, use of this
;; software will be governed by the Apache License, Version 2.0, included in
;; the file licenses/APL2.txt.
(setq-default indent-tabs-mode nil)

(defvar erlang-dirs '("/usr/local/lib/erlang" "/usr/lib/erlang"))
(defvar erlang-root-dir nil)

(dolist (dir erlang-dirs)
  (when (file-accessible-directory-p dir)
    (setq erlang-root-dir dir)))

(unless erlang-root-dir
  (error "Couldn't find erlang installation. Searched in %s" erlang-dirs))

(defconst erlang-lib-dir
  (concat (file-name-as-directory erlang-root-dir) "lib"))
(defconst erlang-tools-dir
  (and (file-accessible-directory-p erlang-lib-dir)
       (concat (file-name-as-directory erlang-lib-dir)
               (car (directory-files erlang-lib-dir nil "^tools-.*")))))
(defconst erlang-emacs-dir
  (concat (file-name-as-directory erlang-tools-dir) "emacs"))

(defun do-indent (path)
  (princ (format "Indending %s\n" path))

  (find-file path)
  (erlang-mode)
  (indent-region (point-min) (point-max))
  (save-buffer)
  (kill-buffer))

(defun get-paths (l)
  (cdr (cdr (cdr l))))

(when (file-accessible-directory-p erlang-emacs-dir)
  (add-to-list 'load-path erlang-emacs-dir)
  (require 'erlang)
  (dolist (path (get-paths command-line-args))
    (do-indent path)))
