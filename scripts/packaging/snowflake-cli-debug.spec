%global debug_package %{nil}
%global _enable_debug_package 1
%global _include_gdb_index 1

Name:           snowflake-cli-debug
Version:        %{version}
Release:        1%{?dist}
Summary:        Snowflake CLI debug build with debugging symbols

License:        Apache-2.0
URL:            https://github.com/snowflakedb/snowflake-cli
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  python3-devel >= 3.10
BuildRequires:  python3-pip
BuildRequires:  python3-setuptools
BuildRequires:  gcc
BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  rust
BuildRequires:  cargo
BuildRequires:  git
BuildRequires:  gdb

Requires:       python3 >= 3.10
Requires:       gdb

# Provide debuginfo package
%package debuginfo
Summary:        Debug information for %{name}
Group:          Development/Debug
AutoReqProv:    0
Requires:       %{name} = %{version}-%{release}

%description
Snowflake CLI tool for developers with debugging symbols and GDB support.
This is a debug build optimized for troubleshooting and development purposes.

%description debuginfo
This package provides debug information for %{name}.
Debug information is useful when developing applications that use this
package or when debugging this package itself.

%prep
%autosetup -n %{name}-%{version}

%build
# Set debug build flags
export CFLAGS="%{optflags} -g -O1"
export CXXFLAGS="%{optflags} -g -O1"
export LDFLAGS="%{__global_ldflags} -g"
export RUSTFLAGS="-C opt-level=1 -C debuginfo=2 -C lto=off"
export CARGO_BUILD_JOBS=1

# Install hatch in build environment
python3 -m pip install --user hatch

# Build the debug binary
export PATH="$HOME/.local/bin:$PATH"
hatch -e packaging run build-debug-binary-low-memory

%install
# Create directories
mkdir -p %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_datadir}/%{name}
mkdir -p %{buildroot}%{_docdir}/%{name}
mkdir -p %{buildroot}%{_mandir}/man1

# Install the debug binary
install -m 755 dist/binary/snow-%{version} %{buildroot}%{_bindir}/snow-debug

# Install documentation
install -m 644 DEBUG_BUILD.md %{buildroot}%{_docdir}/%{name}/
install -m 644 FEDORA_QUICK_START.md %{buildroot}%{_docdir}/%{name}/
install -m 644 README.md %{buildroot}%{_docdir}/%{name}/

# Create a simple man page
cat > %{buildroot}%{_mandir}/man1/snow-debug.1 << 'EOF'
.TH SNOW-DEBUG 1 "$(date +'%B %Y')" "snowflake-cli-debug %{version}" "User Commands"
.SH NAME
snow-debug \- Snowflake CLI debug build with debugging symbols
.SH SYNOPSIS
.B snow-debug
[\fIGLOBAL_OPTIONS\fR] \fICOMMAND\fR [\fICOMMAND_OPTIONS\fR]
.SH DESCRIPTION
Debug build of the Snowflake CLI tool for developers. This version includes
debugging symbols and is optimized for troubleshooting with GDB.
.SH OPTIONS
.TP
.B \-\-help
Show help message and exit
.TP
.B \-\-version
Show version information
.SH DEBUGGING
To debug this application with GDB:
.PP
.nf
.RS
gdb /usr/bin/snow-debug
(gdb) run --help
.RE
.fi
.SH FILES
.TP
.I /usr/share/doc/snowflake-cli-debug/DEBUG_BUILD.md
Comprehensive debugging documentation
.TP
.I /usr/share/doc/snowflake-cli-debug/FEDORA_QUICK_START.md
Fedora-specific setup and debugging guide
.SH SEE ALSO
.BR gdb (1),
.BR python3 (1)
.SH AUTHOR
Snowflake Inc.
EOF

%files
%license LICENSE
%doc %{_docdir}/%{name}/
%{_bindir}/snow-debug
%{_mandir}/man1/snow-debug.1*

%files debuginfo
%{_usr}/lib/debug%{_bindir}/snow-debug*.debug

%post
echo "Snowflake CLI debug build installed successfully!"
echo "To debug: gdb /usr/bin/snow-debug"
echo "Documentation: /usr/share/doc/%{name}/"

%postun
if [ $1 -eq 0 ]; then
    echo "Snowflake CLI debug build removed."
fi

%changelog
* %{date} Snowflake Inc. <%{packager_email}> - %{version}-1
- Debug build with GDB support
- Includes debugging symbols and debuginfo package
- Optimized for low-memory systems
- Fedora-specific RPM packaging
