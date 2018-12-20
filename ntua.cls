\ProvidesClass{ntua}[2018/10/27 NTUA report class]

\LoadClass[12pt]{article}
% greek language
\RequirePackage[utf8]{inputenc}
\RequirePackage[T1]{fontenc}
\RequirePackage[greek,english]{babel}
\RequirePackage{alphabeta}

% margins
\RequirePackage[margin=2.5cm]{geometry}

% \thetitle \theauthor \thedate
\RequirePackage{titling}

% section manipulation
\RequirePackage{titlesec}
\RequirePackage[dvipsnames]{xcolor}

% header footer
\RequirePackage{fancyhdr}

% graphics
\RequirePackage{graphicx}

% float objects manipulation (adds H option)
\RequirePackage{float}

% math packs
\RequirePackage{amsmath}
\RequirePackage{amssymb}

% programming languages
\RequirePackage{minted}

\colorlet{darkcyan}{blue!60!black}

\addto\captionsenglish{\renewcommand{\contentsname}{Περιεχόμενα}}

\newcommand{\aboveRule}{
	\rule[.5\baselineskip]{\textwidth}{.5pt}
	\par\vspace{.5cm}
}

\newcommand{\belowRule}{
	\par\vspace{.5cm}
	\rule[.5\baselineskip]{\textwidth}{.5pt}
}

\titleformat{\section}[hang]
	{\Large\bfseries\scshape\raggedright}
	{\thesection}{.5em}{$|$\hspace{.5em}}
	
\titleformat{\subsection}[hang]
	{\large\bfseries\scshape\raggedright}
	{\thesubsection}{.5em}{$|$\hspace{.5em}}
	
\titleformat{\subsubsection}[hang]
	{\normalsize\bfseries\scshape\raggedright}
	{\thesubsubsection}{.5em}{$|$\hspace{.5em}}